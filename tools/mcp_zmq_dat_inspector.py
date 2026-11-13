#!/usr/bin/env python3
"""
tools/mcp_zmq_dat_inspector.py

High-Performance C++ / ZeroMQ / Qt Data Inspector & Verification MCP Server.
Provides tools to:
1. Inspect binary .dat files with SOP(0x0D0D) / EOP(0x0A0A) frame verification.
2. Compare two recording/replay sessions byte-by-byte across topics.
3. Publish mock ZeroMQ stream cycling across video, hit, ssr, and adsb.
"""

import os
import sys
import glob
import time
import struct
from typing import Dict, Any, List, Optional
import zmq
from fastmcp import FastMCP

# Initialize FastMCP Server
mcp = FastMCP("zmq-dat-inspector")

HEADER_FMT_LE = "<QH"  # uint64 timestamp, uint16 data_len
HEADER_FMT_BE = ">QH"
HEADER_SIZE = 10  # 8 bytes uint64 + 2 bytes uint16

SOP_VALUE = 0x0D0D
EOP_VALUE = 0x0A0A

TOPIC_CONFIGS = [
    ("video", 0x01, 128),
    ("hit",   0x02, 64),
    ("ssr",   0x10, 96),
    ("adsb",  0xF1, 80)
]


def _read_packets_from_file(file_path: str, endian: str = "little"):
    """
    Generator yielding (time_stamp, data_len, frame_rdp, is_valid_sop_eop, sop, eop, error_msg)
    """
    hdr_fmt = HEADER_FMT_LE if endian.lower() == "little" else HEADER_FMT_BE
    u16_fmt = "<H" if endian.lower() == "little" else ">H"

    with open(file_path, "rb") as f:
        while True:
            hdr_bytes = f.read(HEADER_SIZE)
            if not hdr_bytes:
                break
            if len(hdr_bytes) < HEADER_SIZE:
                yield None, len(hdr_bytes), None, False, 0, 0, f"Truncated header ({len(hdr_bytes)} bytes)"
                break

            time_stamp, data_len = struct.unpack(hdr_fmt, hdr_bytes)
            frame_rdp = f.read(data_len)
            if len(frame_rdp) < data_len:
                yield time_stamp, data_len, frame_rdp, False, 0, 0, f"Truncated frame ({len(frame_rdp)}/{data_len} bytes)"
                break

            if data_len < 4:
                yield time_stamp, data_len, frame_rdp, False, 0, 0, "Frame length too short for SOP/EOP"
                continue

            sop = struct.unpack(u16_fmt, frame_rdp[:2])[0]
            eop = struct.unpack(u16_fmt, frame_rdp[-2:])[0]
            is_valid = (sop == SOP_VALUE) and (eop == EOP_VALUE)

            yield time_stamp, data_len, frame_rdp, is_valid, sop, eop, None


@mcp.tool()
def inspect_dat_file(file_path: str, endian: str = "little", max_packets: int = 10) -> Dict[str, Any]:
    """
    Reads binary .dat files with back-to-back layout [time_stamp: uint64 (8B)][data_len: uint16 (2B)][frame_RDP: data_len bytes].
    Verifies SOP == 0x0D0D and EOP == 0x0A0A, checks monotonic timestamps,
    and returns packet statistics and sample metadata.
    """
    if not os.path.exists(file_path):
        return {
            "error": f"File not found: {file_path}",
            "file_path": file_path,
            "total_packets": 0,
            "corrupted_packets": 0,
            "out_of_order_packets": 0
        }

    file_size = os.path.getsize(file_path)
    total_packets = 0
    corrupted_packets = 0
    out_of_order_packets = 0
    prev_timestamp: Optional[int] = None
    first_timestamp: Optional[int] = None
    last_timestamp: Optional[int] = None
    sample_packets: List[Dict[str, Any]] = []

    for ts, data_len, frame, is_valid, sop, eop, err in _read_packets_from_file(file_path, endian):
        total_packets += 1

        if err is not None or not is_valid:
            corrupted_packets += 1

        if ts is not None:
            if first_timestamp is None:
                first_timestamp = ts
            last_timestamp = ts

            if prev_timestamp is not None and ts < prev_timestamp:
                out_of_order_packets += 1
            prev_timestamp = ts

        if len(sample_packets) < max_packets:
            sample_info: Dict[str, Any] = {
                "packet_index": total_packets,
                "timestamp": ts,
                "data_len": data_len,
                "sop_hex": f"0x{sop:04X}" if sop else None,
                "eop_hex": f"0x{eop:04X}" if eop else None,
                "is_valid": is_valid,
                "error": err
            }
            if frame is not None and len(frame) >= 10:
                u16_fmt = "<H" if endian.lower() == "little" else ">H"
                seq = struct.unpack(u16_fmt, frame[2:4])[0]
                src_id = frame[4]
                msg_type = frame[5]
                sample_info.update({
                    "seq": seq,
                    "src_id": f"0x{src_id:02X}",
                    "type": f"0x{msg_type:02X}"
                })
            sample_packets.append(sample_info)

    return {
        "file_path": file_path,
        "file_size_bytes": file_size,
        "endian": endian,
        "total_packets": total_packets,
        "valid_packets": total_packets - corrupted_packets,
        "corrupted_packets": corrupted_packets,
        "out_of_order_packets": out_of_order_packets,
        "first_timestamp": first_timestamp,
        "last_timestamp": last_timestamp,
        "sample_packets": sample_packets
    }


@mcp.tool()
def compare_sessions_byte_by_byte(session1_zdata_dir: str, session2_zdata_dir: str, endian: str = "little") -> Dict[str, Any]:
    """
    For each topic ('video', 'hit', 'ssr', 'adsb'), reads all <topic>/<topic>_*.dat files
    in chronological order from both sessions, strips the 8-byte time_stamp,
    and strictly compares [data_len u16] + [frame_RDP] packet-by-packet.
    Returns exact match status and counts per topic.
    """
    topics = ["video", "hit", "ssr", "adsb"]
    hdr_fmt = HEADER_FMT_LE if endian.lower() == "little" else HEADER_FMT_BE

    overall_match = True
    topic_results: Dict[str, Any] = {}

    def extract_packets(zdata_dir: str, topic: str) -> List[bytes]:
        topic_dir = os.path.join(zdata_dir, topic)
        if not os.path.isdir(topic_dir):
            return []
        dat_files = sorted(glob.glob(os.path.join(topic_dir, f"{topic}_*.dat")))
        packets: List[bytes] = []

        for fpath in dat_files:
            with open(fpath, "rb") as f:
                while True:
                    hdr = f.read(HEADER_SIZE)
                    if not hdr or len(hdr) < HEADER_SIZE:
                        break
                    _, data_len = struct.unpack(hdr_fmt, hdr)
                    frame = f.read(data_len)
                    if len(frame) < data_len:
                        break
                    # Keep [data_len u16 (2B)] + [frame_RDP (data_len B)]
                    packets.append(hdr[8:10] + frame)
        return packets

    for topic in topics:
        s1_pkts = extract_packets(session1_zdata_dir, topic)
        s2_pkts = extract_packets(session2_zdata_dir, topic)

        count_s1 = len(s1_pkts)
        count_s2 = len(s2_pkts)

        matched = 0
        mismatches = 0
        first_mismatch: Optional[Dict[str, Any]] = None

        min_len = min(count_s1, count_s2)
        for i in range(min_len):
            if s1_pkts[i] == s2_pkts[i]:
                matched += 1
            else:
                mismatches += 1
                if first_mismatch is None:
                    first_mismatch = {
                        "packet_index": i,
                        "session1_len": len(s1_pkts[i]),
                        "session2_len": len(s2_pkts[i]),
                        "session1_hex_preview": s1_pkts[i][:16].hex(),
                        "session2_hex_preview": s2_pkts[i][:16].hex()
                    }

        if count_s1 != count_s2:
            mismatches += abs(count_s1 - count_s2)
            if first_mismatch is None:
                first_mismatch = {
                    "error": f"Packet count mismatch: s1={count_s1}, s2={count_s2}"
                }

        if count_s1 == 0 and count_s2 == 0:
            topic_match = True
        else:
            topic_match = (mismatches == 0) and (count_s1 == count_s2)
        if not topic_match:
            overall_match = False

        topic_results[topic] = {
            "match": topic_match,
            "session1_packets": count_s1,
            "session2_packets": count_s2,
            "matched_packets": matched,
            "mismatches": mismatches,
            "first_mismatch": first_mismatch
        }

    return {
        "exact_match": overall_match,
        "session1_zdata_dir": session1_zdata_dir,
        "session2_zdata_dir": session2_zdata_dir,
        "endian": endian,
        "topics": topic_results
    }


@mcp.tool()
def publish_mock_zmq_stream(
    bind_endpoint: str = "tcp://192.168.5.21:30003",
    num_messages: int = 2000,
    endian: str = "little",
    interval_us: int = 200
) -> Dict[str, Any]:
    """
    Binds a zmq.PUB socket to bind_endpoint, sleeps 1.0s for subscribers to connect,
    and sends num_messages multipart messages [topic_bytes, frame_rdp] cycling across
    video (TYPE 0x01), hit (TYPE 0x02), ssr (TYPE 0x10), and adsb (TYPE 0xF1).
    Each frame_rdp follows:
    SOP(0x0D0D u16) | SEQ(u16) | SRC_ID(u8) | TYPE(u8) | LEN(u16) | DATA(LEN bytes) | CHKSUM(u16) | EOP(0x0A0A u16)
    """
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.setsockopt(zmq.LINGER, 500)

    try:
        socket.bind(bind_endpoint)
    except Exception as e:
        context.term()
        return {"error": f"Failed to bind to {bind_endpoint}: {str(e)}", "messages_sent": 0}

    # Allow subscribers 1.0s to connect (mitigate slow-joiner symptom)
    time.sleep(1.0)

    is_le = (endian.lower() == "little")
    hdr_fmt = "<HHBBH" if is_le else ">HHBBH"
    tail_fmt = "<HH" if is_le else ">HH"

    topic_counts = {t[0]: 0 for t in TOPIC_CONFIGS}
    src_id = 0x21

    interval_sec = interval_us / 1_000_000.0 if interval_us > 0 else 0.0

    t_start = time.perf_counter()
    for i in range(num_messages):
        topic_name, type_byte, payload_len = TOPIC_CONFIGS[i % len(TOPIC_CONFIGS)]
        topic_bytes = topic_name.encode("utf-8")

        seq = i % 65536
        header_bytes = struct.pack(hdr_fmt, SOP_VALUE, seq, src_id, type_byte, payload_len)
        data_bytes = bytes([(i + b) & 0xFF for b in range(payload_len)])

        chksum = sum(header_bytes + data_bytes) & 0xFFFF
        tail_bytes = struct.pack(tail_fmt, chksum, EOP_VALUE)

        frame_rdp = header_bytes + data_bytes + tail_bytes

        socket.send_multipart([topic_bytes, frame_rdp])
        topic_counts[topic_name] += 1

        if interval_sec > 0:
            target_time = t_start + (i + 1) * interval_sec
            while time.perf_counter() < target_time:
                pass

    t_elapsed = time.perf_counter() - t_start

    # Clean shutdown
    time.sleep(0.05)
    socket.close()
    context.term()

    return {
        "status": "success",
        "bind_endpoint": bind_endpoint,
        "num_messages_sent": num_messages,
        "elapsed_seconds": round(t_elapsed, 4),
        "rate_msg_sec": round(num_messages / t_elapsed if t_elapsed > 0 else 0, 1),
        "endian": endian,
        "interval_us": interval_us,
        "topic_counts": topic_counts
    }


if __name__ == "__main__":
    # If invoked directly with CLI subcommands for verification/testing
    if len(sys.argv) > 1 and sys.argv[1] in ("--help", "-h"):
        print("Usage: python mcp_zmq_dat_inspector.py [run | --inspect <file> | --publish <endpoint>]")
        sys.exit(0)
    elif len(sys.argv) > 1 and sys.argv[1] == "--test":
        print("Running internal smoke test...")
        import asyncio
        tools = asyncio.run(mcp.list_tools())
        print("FastMCP tools registered:", [t.name for t in tools])
        sys.exit(0)
    else:
        # Default behavior: run stdio FastMCP server
        mcp.run()
