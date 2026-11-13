#!/usr/bin/env python3
"""
test_harness.py - Radar ZMQ Recorder & Replay QA Test Harness
==============================================================
Standalone, zero-external-dependency QA utility using only Python standard library:
(socket, struct, time, os, sys, glob, argparse, hashlib, math, signal).

Provides 3 core operating modes:
1. mock-tcp         : Acts as a TCP Server simulating radar telemetry source (appmrepl).
                      Generates valid RDP frames and stresses the ZMQ Bridge framing logic
                      using normal, fragmented, and coalesced TCP transmissions.
2. verify-dat       : Inspects and strictly verifies .dat binary recording files.
                      Checks 0-byte files, Little-Endian layout, monotonic UTC timestamps,
                      SOP/EOP markers, TYPE-to-topic mapping, and package integrity.
3. compare-sessions : Closed-loop byte-by-byte verification between original recording
                      and replayed session. Compares exact packet payloads and measures
                      replay timing precision / jitter.
"""

import os
import sys
import glob
import time
import math
import struct
import socket
import signal
import hashlib
import argparse
from typing import Dict, List, Tuple, Optional, Any

# -----------------------------------------------------------------------------
# Radar Protocol Constants & Configuration
# -----------------------------------------------------------------------------
SOP_MARKER = 0x0D0D
EOP_MARKER = 0x0A0A
DEFAULT_SRC_ID = 0x21

DAT_HEADER_SIZE = 10  # 8B time_stamp (uint64) + 2B data_len (uint16)
MIN_RDP_FRAME_LEN = 12 # SOP(2)+SEQ(2)+SRC_ID(1)+TYPE(1)+LEN(2)+CHKSUM(2)+EOP(2)

TYPE_TO_TOPIC = {
    0x01: "video",
    0x02: "hit",
    0x10: "ssr",
    0x11: "ssr",
    0xF1: "adsb",
}

TOPIC_TO_ALLOWED_TYPES = {
    "video": {0x01},
    "hit": {0x02},
    "ssr": {0x10, 0x11},
    "adsb": {0xF1},
}

TOPIC_CONFIGS = [
    ("video", 0x01, 128),
    ("hit",   0x02, 64),
    ("ssr",   0x10, 96),
    ("ssr",   0x11, 96),
    ("adsb",  0xF1, 80),
]

ALL_TOPICS = ["video", "hit", "ssr", "adsb"]

# Global exit flag for graceful shutdown on SIGINT/SIGTERM
g_stop_requested = False


def _sig_handler(signum, frame):
    global g_stop_requested
    g_stop_requested = True
    print("\n[Signal] Interrupt caught, completing gracefully...")


# -----------------------------------------------------------------------------
# Common Path & Utility Functions
# -----------------------------------------------------------------------------
def resolve_zdata_dir(path: str) -> Optional[str]:
    """
    Locates the 'zdata' directory inside the provided path or verifies
    if the path is already the zdata directory containing topic folders.
    """
    if not os.path.exists(path):
        return None

    # Case 1: path itself is zdata or contains topic directories directly
    has_topics = any(os.path.isdir(os.path.join(path, t)) for t in ALL_TOPICS)
    if has_topics:
        return os.path.abspath(path)

    # Case 2: path/zdata exists
    nested_zdata = os.path.join(path, "zdata")
    if os.path.isdir(nested_zdata):
        return os.path.abspath(nested_zdata)

    # Case 3: search child subdirectories for a zdata folder
    candidates = sorted(glob.glob(os.path.join(path, "*", "zdata")))
    if candidates:
        return os.path.abspath(candidates[-1])

    return None


def build_rdp_frame(seq: int, msg_type: int, payload_len: int, src_id: int = DEFAULT_SRC_ID) -> bytes:
    """
    Constructs an RDP binary frame according to the protocol:
    [SOP 0x0D0D (2B)] [SEQ u16 (2B)] [SRC_ID u8 (1B)] [TYPE u8 (1B)] [LEN u16 (2B)]
    [DATA (LEN Bytes)] [CHKSUM u16 (2B)] [EOP 0x0A0A (2B)]
    All multi-byte numeric fields are encoded as Little-Endian (<).
    """
    header = struct.pack("<HHBBH", SOP_MARKER, seq & 0xFFFF, src_id & 0xFF, msg_type & 0xFF, payload_len & 0xFFFF)
    # Generate deterministic pseudo-random bytes based on seq & type for reproducible testing
    seed = (seq * 31 + msg_type) & 0xFF
    data = bytes([(seed + i) & 0xFF for i in range(payload_len)])
    chksum = sum(header + data) & 0xFFFF
    tail = struct.pack("<HH", chksum, EOP_MARKER)
    return header + data + tail


# =============================================================================
# 1. MODE: mock-tcp (Simulate appmrepl TCP Server)
# =============================================================================
def run_mock_tcp(args: argparse.Namespace) -> int:
    global g_stop_requested
    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    host = args.host
    port = args.port
    duration_sec = args.duration
    target_rate = max(1, args.rate) # packets per second
    interval_sec = 1.0 / target_rate

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        server_sock.bind((host, port))
        server_sock.listen(1)
        server_sock.settimeout(1.0)
    except Exception as e:
        print(f"[mock-tcp] ERROR: Could not bind TCP socket to {host}:{port} - {e}")
        return 1

    print("=" * 70)
    print("                     MOCK TCP RADAR SOURCE (appmrepl)                  ")
    print("=" * 70)
    print(f"  Bind Address   : {host}:{port}")
    print(f"  Target Rate    : {target_rate} pkts/sec (interval {interval_sec*1000:.2f} ms)")
    print(f"  Duration Limit : {duration_sec}s (0 = infinite)")
    print("  Framing Tests  : (a) Normal  (b) Fragmented (split 10ms)  (c) Coalesced (3 pkts)")
    print("  Waiting for ZMQ Bridge client connection...")
    print("=" * 70)

    client_sock = None
    client_addr = None

    while not g_stop_requested:
        try:
            client_sock, client_addr = server_sock.accept()
            print(f"[mock-tcp] Accepted connection from {client_addr[0]}:{client_addr[1]}")
            # Allow 500ms for downstream subscribers in pipeline to establish ZMQ filter
            time.sleep(0.5)
            break
        except socket.timeout:
            continue
        except Exception as e:
            if not g_stop_requested:
                print(f"[mock-tcp] Accept error: {e}")
            return 1

    if g_stop_requested or not client_sock:
        server_sock.close()
        print("[mock-tcp] Stopped before any client connected.")
        return 0

    # Transmission counters
    stats_frames = {t: 0 for t in ALL_TOPICS}
    stats_bytes = {t: 0 for t in ALL_TOPICS}
    pattern_counts = {"normal": 0, "fragmented": 0, "coalesced": 0}

    seq = 0
    t_start = time.perf_counter()
    pattern_idx = 0

    try:
        client_sock.settimeout(2.0)
        while not g_stop_requested:
            elapsed = time.perf_counter() - t_start
            if duration_sec > 0 and elapsed >= duration_sec:
                print(f"[mock-tcp] Duration limit of {duration_sec}s reached.")
                break

            # Transmission Pattern rotation:
            # 0: Normal single frame
            # 1: Fragmented into 2 pieces separated by 10ms
            # 2: Coalesced 3 frames in a single send call
            pattern_mode = pattern_idx % 3
            pattern_idx += 1

            if pattern_mode == 0:
                # (a) Normal single frame
                topic_name, type_byte, payload_len = TOPIC_CONFIGS[seq % len(TOPIC_CONFIGS)]
                frame = build_rdp_frame(seq, type_byte, payload_len)
                seq += 1

                client_sock.sendall(frame)
                stats_frames[topic_name] += 1
                stats_bytes[topic_name] += len(frame)
                pattern_counts["normal"] += 1

            elif pattern_mode == 1:
                # (b) Fragmented frame (split into 2 parts separated by 10ms)
                topic_name, type_byte, payload_len = TOPIC_CONFIGS[seq % len(TOPIC_CONFIGS)]
                frame = build_rdp_frame(seq, type_byte, payload_len)
                seq += 1

                # Split frame across middle
                split_point = max(1, len(frame) // 2)
                part1 = frame[:split_point]
                part2 = frame[split_point:]

                client_sock.sendall(part1)
                time.sleep(0.010)  # 10 ms delay between fragments
                client_sock.sendall(part2)

                stats_frames[topic_name] += 1
                stats_bytes[topic_name] += len(frame)
                pattern_counts["fragmented"] += 1

            else:
                # (c) Coalesced frames (3 frames sent in one single buffer)
                frames_buffer = bytearray()
                for _ in range(3):
                    topic_name, type_byte, payload_len = TOPIC_CONFIGS[seq % len(TOPIC_CONFIGS)]
                    frame = build_rdp_frame(seq, type_byte, payload_len)
                    seq += 1

                    frames_buffer.extend(frame)
                    stats_frames[topic_name] += 1
                    stats_bytes[topic_name] += len(frame)

                client_sock.sendall(frames_buffer)
                pattern_counts["coalesced"] += 3

            # Rate pacing
            if interval_sec > 0:
                time.sleep(interval_sec)

    except (BrokenPipeError, ConnectionResetError):
        print(f"[mock-tcp] Client {client_addr} disconnected.")
    except Exception as e:
        print(f"[mock-tcp] Transmission exception: {e}")
    finally:
        try:
            client_sock.close()
        except Exception:
            pass
        server_sock.close()

    t_total = time.perf_counter() - t_start
    total_frames = sum(stats_frames.values())
    total_bytes = sum(stats_bytes.values())

    print("\n" + "=" * 70)
    print("                     MOCK TCP TRANSMISSION SUMMARY                     ")
    print("=" * 70)
    print(f"  Duration Elapsed : {t_total:.2f} s")
    print(f"  Total Frames Sent: {total_frames}")
    print(f"  Total Wire Bytes : {total_bytes:,} bytes")
    print(f"  Transmit Patterns: Normal={pattern_counts['normal']} | "
          f"Fragmented={pattern_counts['fragmented']} | "
          f"Coalesced={pattern_counts['coalesced']}")
    rate_actual = (total_frames / t_total) if t_total > 0 else 0
    print(f"  Actual Rate      : {rate_actual:.1f} frames/sec")
    print("-" * 70)
    print(f"{'Topic':<10} {'Frames Sent':<16} {'Bytes Sent':<16} {'Share':<10}")
    print("-" * 70)
    for topic in ALL_TOPICS:
        f_count = stats_frames[topic]
        b_count = stats_bytes[topic]
        pct = (b_count / total_bytes * 100.0) if total_bytes > 0 else 0.0
        print(f"{topic:<10} {f_count:<16} {b_count:<16} {pct:>5.1f}%")
    print("-" * 70)
    print(f"{'TOTAL':<10} {total_frames:<16} {total_bytes:<16} 100.0%")
    print("=" * 70 + "\n")

    return 0


# =============================================================================
# 2. MODE: verify-dat (Binary File & Protocol Integrity Checker)
# =============================================================================
def run_verify_dat(args: argparse.Namespace) -> int:
    zdata_dir = resolve_zdata_dir(args.session_dir)
    if not zdata_dir:
        print(f"[verify-dat] ERROR: Could not locate a valid 'zdata' directory at '{args.session_dir}'")
        return 1

    print("=" * 75)
    print("                     RECORDING BINARY INTEGRITY AUDIT                  ")
    print("=" * 75)
    print(f"  Inspecting Target ZData : {zdata_dir}")
    print("  Verification Checks     : No 0-byte files | Monotonic u64 UTC |")
    print("                            SOP(0x0D0D) & EOP(0x0A0A) | TYPE mapping | Truncation")
    print("=" * 75)

    overall_pass = True
    session_stats: Dict[str, Dict[str, Any]] = {}
    validation_errors: List[str] = []

    for topic in ALL_TOPICS:
        topic_dir = os.path.join(zdata_dir, topic)
        dat_files = sorted(glob.glob(os.path.join(topic_dir, f"{topic}_*.dat")))

        t_stats = {
            "files_count": len(dat_files),
            "total_packages": 0,
            "valid_packages": 0,
            "corrupted_packages": 0,
            "total_bytes": 0,
            "empty_files": 0,
            "status": "PASS"
        }

        prev_timestamp: Optional[int] = None

        if not dat_files:
            # Note: A topic might have 0 files if no telemetry arrived (Lazy creation).
            # We flag it as informative unless expected.
            t_stats["status"] = "EMPTY_TOPIC"
            session_stats[topic] = t_stats
            continue

        for fpath in dat_files:
            file_size = os.path.getsize(fpath)
            t_stats["total_bytes"] += file_size
            fname = os.path.basename(fpath)

            # Rule 1: Check for 0-byte file (Lazy creation violation)
            if file_size == 0:
                t_stats["empty_files"] += 1
                t_stats["status"] = "FAIL"
                overall_pass = False
                err_msg = f"[{topic}] VIOLATION: Empty file detected (0 bytes): {fname}"
                validation_errors.append(err_msg)
                continue

            with open(fpath, "rb") as f:
                offset = 0
                pkg_in_file = 0

                while True:
                    hdr_bytes = f.read(DAT_HEADER_SIZE)
                    if not hdr_bytes:
                        break  # Clean EOF

                    if len(hdr_bytes) < DAT_HEADER_SIZE:
                        t_stats["corrupted_packages"] += 1
                        t_stats["status"] = "FAIL"
                        overall_pass = False
                        validation_errors.append(
                            f"[{topic}][{fname}] TRUNCATED HEADER at offset {offset}: "
                            f"got {len(hdr_bytes)} bytes, expected {DAT_HEADER_SIZE} bytes."
                        )
                        break

                    # Unpack 10-byte fixed header: uint64 time_stamp (8B), uint16 data_len (2B)
                    ts_us, data_len = struct.unpack("<QH", hdr_bytes)
                    offset += DAT_HEADER_SIZE
                    t_stats["total_packages"] += 1
                    pkg_in_file += 1

                    # Read payload
                    frame_bytes = f.read(data_len)
                    if len(frame_bytes) < data_len:
                        t_stats["corrupted_packages"] += 1
                        t_stats["status"] = "FAIL"
                        overall_pass = False
                        validation_errors.append(
                            f"[{topic}][{fname}] TRUNCATED FRAME at offset {offset}: "
                            f"got {len(frame_bytes)} bytes, expected {data_len} bytes."
                        )
                        break
                    offset += data_len

                    # Check Timestamp validity & monotonic non-decreasing
                    # Valid range: UTC microseconds roughly > year 2020 (1.5e15 us)
                    pkg_corrupted = False

                    if ts_us < 1_500_000_000_000_000:
                        pkg_corrupted = True
                        overall_pass = False
                        validation_errors.append(
                            f"[{topic}][{fname}] INVALID TIMESTAMP: {ts_us} us at pkg #{pkg_in_file}"
                        )

                    if prev_timestamp is not None and ts_us < prev_timestamp:
                        pkg_corrupted = True
                        overall_pass = False
                        validation_errors.append(
                            f"[{topic}][{fname}] NON-MONOTONIC TIMESTAMP: "
                            f"current {ts_us} < prev {prev_timestamp} (diff: {prev_timestamp - ts_us} us)"
                        )
                    prev_timestamp = ts_us

                    # Check RDP Frame markers
                    if data_len < MIN_RDP_FRAME_LEN:
                        pkg_corrupted = True
                        t_stats["corrupted_packages"] += 1
                        t_stats["status"] = "FAIL"
                        overall_pass = False
                        validation_errors.append(
                            f"[{topic}][{fname}] FRAME TOO SHORT: data_len={data_len} < {MIN_RDP_FRAME_LEN}"
                        )
                        continue

                    sop = struct.unpack("<H", frame_bytes[:2])[0]
                    eop = struct.unpack("<H", frame_bytes[-2:])[0]

                    if sop != SOP_MARKER:
                        pkg_corrupted = True
                        overall_pass = False
                        validation_errors.append(
                            f"[{topic}][{fname}] INVALID SOP: 0x{sop:04X} (expected 0x{SOP_MARKER:04X})"
                        )

                    if eop != EOP_MARKER:
                        pkg_corrupted = True
                        overall_pass = False
                        validation_errors.append(
                            f"[{topic}][{fname}] INVALID EOP: 0x{eop:04X} (expected 0x{EOP_MARKER:04X})"
                        )

                    # Check TYPE-to-Topic mapping
                    type_byte = frame_bytes[5]
                    allowed_types = TOPIC_TO_ALLOWED_TYPES.get(topic, set())
                    if type_byte not in allowed_types:
                        pkg_corrupted = True
                        overall_pass = False
                        validation_errors.append(
                            f"[{topic}][{fname}] TYPE MISMATCH: byte=0x{type_byte:02X} not allowed in topic '{topic}'"
                        )

                    # Check internal LEN field consistency
                    internal_len = struct.unpack("<H", frame_bytes[6:8])[0]
                    expected_frame_len = internal_len + 12
                    if data_len != expected_frame_len:
                        pkg_corrupted = True
                        overall_pass = False
                        validation_errors.append(
                            f"[{topic}][{fname}] PAYLOAD LEN INCONSISTENT: "
                            f"internal LEN={internal_len} => expected {expected_frame_len}B, but data_len={data_len}B"
                        )

                    if pkg_corrupted:
                        t_stats["corrupted_packages"] += 1
                        t_stats["status"] = "FAIL"
                    else:
                        t_stats["valid_packages"] += 1

        session_stats[topic] = t_stats

    # Print Verification Report
    print(f"{'Topic':<8} {'Files':<7} {'Total Pkts':<12} {'Valid Pkts':<12} {'Corrupt':<9} {'Bytes On Disk':<15} {'Status'}")
    print("-" * 75)
    total_files = 0
    grand_pkts = 0
    grand_valid = 0
    grand_corrupt = 0
    grand_bytes = 0

    for topic in ALL_TOPICS:
        st = session_stats[topic]
        total_files += st["files_count"]
        grand_pkts += st["total_packages"]
        grand_valid += st["valid_packages"]
        grand_corrupt += st["corrupted_packages"]
        grand_bytes += st["total_bytes"]

        status_str = f"\033[92m{st['status']}\033[0m" if st["status"] == "PASS" else (
            f"\033[93m{st['status']}\033[0m" if st["status"] == "EMPTY_TOPIC" else f"\033[91m{st['status']}\033[0m"
        )
        print(f"{topic:<8} {st['files_count']:<7} {st['total_packages']:<12} {st['valid_packages']:<12} "
              f"{st['corrupted_packages']:<9} {st['total_bytes']:<15,} {status_str}")

    print("-" * 75)
    print(f"{'TOTAL':<8} {total_files:<7} {grand_pkts:<12} {grand_valid:<12} {grand_corrupt:<9} {grand_bytes:<15,}")
    print("=" * 75)

    if validation_errors:
        print("\n>>> DETECTED INTEGRITY FAILURES (First 15 errors):")
        for err in validation_errors[:15]:
            print(f"  [X] {err}")
        if len(validation_errors) > 15:
            print(f"  ... and {len(validation_errors) - 15} additional errors omitted.")
        print("\nOVERALL AUDIT RESULT: \033[91mFAILED\033[0m\n")
        return 1
    else:
        print("\nOVERALL AUDIT RESULT: \033[92mPASSED (100% SPEC COMPLIANT)\033[0m\n")
        return 0


# =============================================================================
# 3. MODE: compare-sessions (Closed-Loop Byte-by-Byte & Timing Verifier)
# =============================================================================
def _extract_session_topic_packages(zdata_dir: str, topic: str) -> List[Tuple[int, int, bytes]]:
    """
    Extracts all packages for a topic across all segment files in chronological order.
    Returns: List of (time_stamp_us, data_len, frame_rdp_bytes)
    """
    topic_dir = os.path.join(zdata_dir, topic)
    if not os.path.isdir(topic_dir):
        return []

    dat_files = sorted(glob.glob(os.path.join(topic_dir, f"{topic}_*.dat")))
    packages: List[Tuple[int, int, bytes]] = []

    for fpath in dat_files:
        with open(fpath, "rb") as f:
            while True:
                hdr = f.read(DAT_HEADER_SIZE)
                if not hdr or len(hdr) < DAT_HEADER_SIZE:
                    break
                ts, data_len = struct.unpack("<QH", hdr)
                frame = f.read(data_len)
                if len(frame) < data_len:
                    break
                packages.append((ts, data_len, frame))
    return packages


def run_compare_sessions(args: argparse.Namespace) -> int:
    zdata1 = resolve_zdata_dir(args.dir1)
    zdata2 = resolve_zdata_dir(args.dir2)

    if not zdata1:
        print(f"[compare-sessions] ERROR: Session 1 path not found: {args.dir1}")
        return 1
    if not zdata2:
        print(f"[compare-sessions] ERROR: Session 2 path not found: {args.dir2}")
        return 1

    print("=" * 80)
    print("              CLOSED-LOOP BYTE-BY-BYTE & REPLAY TIMING VERIFICATION            ")
    print("=" * 80)
    print(f"  Session 1 (Original Recorded) : {zdata1}")
    print(f"  Session 2 (Replayed/Recorded) : {zdata2}")
    print(f"  Verification Criterion        : 100% Exact Byte Match of [data_len + frame_RDP]")
    print(f"  Timing Evaluation             : Microsecond Inter-Packet Interval Drift & Jitter")
    print("=" * 80)

    overall_match = True
    topic_results: Dict[str, Dict[str, Any]] = {}

    for topic in ALL_TOPICS:
        pkts1 = _extract_session_topic_packages(zdata1, topic)
        pkts2 = _extract_session_topic_packages(zdata2, topic)

        count1 = len(pkts1)
        count2 = len(pkts2)
        min_count = min(count1, count2)

        matched_bytes_pkts = 0
        mismatch_count = 0
        first_mismatch_detail = None

        # 1. Byte-by-byte comparison
        for i in range(min_count):
            ts1, len1, frame1 = pkts1[i]
            ts2, len2, frame2 = pkts2[i]

            if len1 == len2 and frame1 == frame2:
                matched_bytes_pkts += 1
            else:
                mismatch_count += 1
                if first_mismatch_detail is None:
                    # Pinpoint first byte difference
                    diff_byte_idx = -1
                    for b_idx in range(min(len(frame1), len(frame2))):
                        if frame1[b_idx] != frame2[b_idx]:
                            diff_byte_idx = b_idx
                            break
                    first_mismatch_detail = {
                        "packet_index": i,
                        "s1_len": len1,
                        "s2_len": len2,
                        "first_diff_offset": diff_byte_idx,
                        "s1_sha256": hashlib.sha256(frame1).hexdigest()[:12],
                        "s2_sha256": hashlib.sha256(frame2).hexdigest()[:12]
                    }

        if count1 != count2:
            mismatch_count += abs(count1 - count2)
            if first_mismatch_detail is None:
                first_mismatch_detail = {
                    "error": f"Total packet count mismatch: S1={count1}, S2={count2}"
                }

        topic_passed = (mismatch_count == 0) and (count1 == count2)
        if not topic_passed:
            overall_match = False

        # 2. Timing Jitter & Pacing Drift calculation
        # Measures delta_t = ts[i] - ts[i-1] between successive packages in both sessions
        timing_errors_us = []
        if min_count > 1:
            for i in range(1, min_count):
                delta1 = pkts1[i][0] - pkts1[i - 1][0]
                delta2 = pkts2[i][0] - pkts2[i - 1][0]
                timing_errors_us.append(abs(delta2 - delta1))

        if timing_errors_us:
            mae_us = sum(timing_errors_us) / len(timing_errors_us)
            max_jitter_us = max(timing_errors_us)
            variance = sum((x - mae_us) ** 2 for x in timing_errors_us) / len(timing_errors_us)
            std_dev_us = math.sqrt(variance)
        else:
            mae_us = 0.0
            max_jitter_us = 0
            std_dev_us = 0.0

        topic_results[topic] = {
            "passed": topic_passed,
            "count1": count1,
            "count2": count2,
            "matched": matched_bytes_pkts,
            "mismatches": mismatch_count,
            "mae_us": mae_us,
            "max_jitter_us": max_jitter_us,
            "std_dev_us": std_dev_us,
            "first_mismatch": first_mismatch_detail
        }

    # Print Comparison Table
    print(f"{'Topic':<8} {'S1 Pkts':<10} {'S2 Pkts':<10} {'Matched':<10} {'Mismatches':<11} "
          f"{'MAE Drift':<12} {'Max Jitter':<12} {'Status'}")
    print("-" * 80)

    total_s1 = 0
    total_s2 = 0
    total_matched = 0
    total_mismatches = 0

    for topic in ALL_TOPICS:
        res = topic_results[topic]
        total_s1 += res["count1"]
        total_s2 += res["count2"]
        total_matched += res["matched"]
        total_mismatches += res["mismatches"]

        status_str = "\033[92mMATCH\033[0m" if res["passed"] else "\033[91mMISMATCH\033[0m"
        mae_str = f"{res['mae_us']:.1f} us"
        jitter_str = f"{res['max_jitter_us']:,} us"

        print(f"{topic:<8} {res['count1']:<10} {res['count2']:<10} {res['matched']:<10} "
              f"{res['mismatches']:<11} {mae_str:<12} {jitter_str:<12} {status_str}")

    print("-" * 80)
    print(f"{'TOTAL':<8} {total_s1:<10} {total_s2:<10} {total_matched:<10} {total_mismatches:<11}")
    print("=" * 80)

    # Detailed Mismatch Output
    if not overall_match:
        print("\n>>> CRITICAL BYTE-LEVEL MISMATCHES FOUND:")
        for topic, res in topic_results.items():
            if not res["passed"]:
                print(f"  [*] Topic '{topic}':")
                d = res["first_mismatch"]
                if d and "first_diff_offset" in d:
                    print(f"      - First mismatch at package index : #{d['packet_index']}")
                    print(f"      - Byte offset within frame        : byte #{d['first_diff_offset']}")
                    print(f"      - S1 Frame SHA256 (first 12)      : {d['s1_sha256']} (len={d['s1_len']})")
                    print(f"      - S2 Frame SHA256 (first 12)      : {d['s2_sha256']} (len={d['s2_len']})")
                elif d and "error" in d:
                    print(f"      - Error: {d['error']}")

        print("\nCLOSED-LOOP VERIFICATION RESULT: \033[91mFAILED (Data divergence detected!)\033[0m\n")
        return 1
    else:
        print("\nCLOSED-LOOP VERIFICATION RESULT: \033[92mPASSED (100% BYTE-EXACT MATCH!)\033[0m\n")
        return 0


# =============================================================================
# CLI Main Entrypoint
# =============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Radar ZMQ Recording & Replay QA Test Harness (Standard Library Only)",
        formatter_class=argparse.RawTextHelpFormatter
    )
    subparsers = parser.add_subparsers(dest="command", required=True, help="Subcommand to execute")

    # 1. mock-tcp
    p_tcp = subparsers.add_parser("mock-tcp", help="Simulate TCP appmrepl radar source streaming raw RDP")
    p_tcp.add_argument("--host", default="127.0.0.1", help="TCP listen host (default: 127.0.0.1)")
    p_tcp.add_argument("--port", type=int, default=8889, help="TCP listen port (default: 8889)")
    p_tcp.add_argument("--duration", type=int, default=30, help="Transmission duration in seconds (0 = infinite, default: 30)")
    p_tcp.add_argument("--rate", type=int, default=100, help="Target transmission rate in frames/sec (default: 100)")

    # 2. verify-dat
    p_verify = subparsers.add_parser("verify-dat", help="Verify integrity of recorded binary .dat session files")
    p_verify.add_argument("--session-dir", required=True, help="Path to session directory or zdata folder")

    # 3. compare-sessions
    p_cmp = subparsers.add_parser("compare-sessions", help="Compare original vs replayed session byte-by-byte")
    p_cmp.add_argument("--dir1", required=True, help="Original recording session directory")
    p_cmp.add_argument("--dir2", required=True, help="Replayed recording session directory")

    args = parser.parse_args()

    if args.command == "mock-tcp":
        sys.exit(run_mock_tcp(args))
    elif args.command == "verify-dat":
        sys.exit(run_verify_dat(args))
    elif args.command == "compare-sessions":
        sys.exit(run_compare_sessions(args))
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
