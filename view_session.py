#!/usr/bin/env python3
"""
view_session.py - Công cụ hiển thị và phân tích dữ liệu file .dat trong session một cách trực quan, dễ nhìn.
Sử dụng 100% thư viện chuẩn của Python, không phụ thuộc vào bất kỳ thư viện ngoài nào.
"""

import os
import sys
import glob
import struct
import argparse
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

# ANSI Color codes for beautiful terminal output
class Colors:
    RESET   = "\033[0m"
    BOLD    = "\033[1m"
    DIM     = "\033[2m"
    RED     = "\033[91m"
    GREEN   = "\033[92m"
    YELLOW  = "\033[93m"
    BLUE    = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN    = "\033[96m"
    WHITE   = "\033[97m"

# Type to topic / description mapping
TYPE_NAMES = {
    0x01: ("video", "Radar Raw Video"),
    0x02: ("hit",   "Target Detection Plot"),
    0x10: ("ssr",   "SSR Mode 3/A Reply"),
    0x11: ("ssr",   "SSR Mode C Reply"),
    0xF1: ("adsb",  "ADS-B 1090MHz Squitter")
}

TOPIC_COLORS = {
    "video": Colors.CYAN,
    "hit":   Colors.GREEN,
    "ssr":   Colors.YELLOW,
    "adsb":  Colors.MAGENTA
}


def format_bytes(num_bytes: int) -> str:
    """Đổi số byte thành chuỗi KB, MB dễ đọc."""
    if num_bytes < 1024:
        return f"{num_bytes} B"
    elif num_bytes < 1024 * 1024:
        return f"{num_bytes / 1024:.2f} KB"
    else:
        return f"{num_bytes / (1024 * 1024):.2f} MB"


def parse_packet(raw_header: bytes, raw_frame: bytes, endian: str = "little") -> Dict[str, Any]:
    """Parse 10-byte header và frame_RDP."""
    hdr_fmt = "<QH" if endian == "little" else ">QH"
    timestamp_us, data_len = struct.unpack(hdr_fmt, raw_header)
    dt_utc = datetime.fromtimestamp(timestamp_us / 1_000_000.0, tz=timezone.utc)

    pkt = {
        "timestamp_us": timestamp_us,
        "datetime_utc": dt_utc.strftime("%Y-%m-%d %H:%M:%S.%f"),
        "data_len": data_len,
        "is_valid": False,
        "error": None,
        "sop": 0,
        "seq": 0,
        "src_id": 0,
        "type": 0,
        "payload_len": 0,
        "chksum": 0,
        "eop": 0,
        "data_bytes": b""
    }

    if len(raw_frame) < data_len:
        pkt["error"] = f"Frame bị cụt ({len(raw_frame)}/{data_len} bytes)"
        return pkt

    if data_len < 12:
        pkt["error"] = f"Frame quá ngắn ({data_len} < 12 bytes)"
        return pkt

    u16_fmt = "<H" if endian == "little" else ">H"
    sop = struct.unpack(u16_fmt, raw_frame[0:2])[0]
    seq = struct.unpack(u16_fmt, raw_frame[2:4])[0]
    src_id = raw_frame[4]
    type_byte = raw_frame[5]
    payload_len = struct.unpack(u16_fmt, raw_frame[6:8])[0]

    chksum = struct.unpack(u16_fmt, raw_frame[-4:-2])[0]
    eop = struct.unpack(u16_fmt, raw_frame[-2:])[0]

    data_payload = raw_frame[8:-4]

    is_valid = (sop == 0x0D0D) and (eop == 0x0A0A)

    pkt.update({
        "sop": sop,
        "seq": seq,
        "src_id": src_id,
        "type": type_byte,
        "payload_len": payload_len,
        "chksum": chksum,
        "eop": eop,
        "is_valid": is_valid,
        "data_bytes": data_payload
    })

    if not is_valid:
        pkt["error"] = f"Sai Marker (SOP=0x{sop:04X}, EOP=0x{eop:04X})"

    return pkt


def read_dat_file(file_path: str, endian: str = "little") -> List[Dict[str, Any]]:
    """Đọc toàn bộ các gói tin trong 1 file .dat."""
    packets = []
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        return packets

    hdr_fmt = "<QH" if endian == "little" else ">QH"
    with open(file_path, "rb") as fp:
        while True:
            raw_hdr = fp.read(10)
            if not raw_hdr or len(raw_hdr) < 10:
                break
            _, data_len = struct.unpack(hdr_fmt, raw_hdr)
            raw_frame = fp.read(data_len)
            pkt = parse_packet(raw_hdr, raw_frame, endian)
            packets.append(pkt)
    return packets


def find_latest_session_dir(base_dir: str) -> Optional[str]:
    """Tìm thư mục session zdata mới nhất."""
    candidates = sorted(glob.glob(os.path.join(base_dir, "**", "zdata"), recursive=True))
    if candidates:
        return candidates[-1]
    zdata_direct = os.path.join(base_dir, "zdata")
    if os.path.exists(zdata_direct):
        return zdata_direct
    return None


def hex_preview(data: bytes, max_len: int = 8) -> str:
    """Tạo chuỗi preview mã Hex của dữ liệu."""
    if not data:
        return "<Rỗng>"
    preview = " ".join(f"{b:02X}" for b in data[:max_len])
    if len(data) > max_len:
        preview += f" ...(+{len(data) - max_len}B)"
    return preview


def main():
    parser = argparse.ArgumentParser(
        description="Hiển thị và phân tích nội dung các file .dat trong session một cách trực quan, đẹp mắt."
    )
    parser.add_argument(
        "session",
        nargs="?",
        default="my_test_session",
        help="Đường dẫn tới thư mục session (mặc định: 'my_test_session')"
    )
    parser.add_argument(
        "--topic",
        choices=["video", "hit", "ssr", "adsb"],
        default=None,
        help="Lọc chỉ hiển thị riêng một topic"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Số lượng gói tin hiển thị mẫu cho mỗi topic (mặc định: 5; dùng --all để xem toàn bộ)"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Hiển thị tất cả gói tin, không giới hạn số lượng"
    )
    parser.add_argument(
        "--timeline",
        action="store_true",
        help="Trộn tất cả các kênh và hiển thị theo đúng trình tự thời gian (như cách Replay phát ra)"
    )
    parser.add_argument(
        "--endian",
        choices=["little", "big"],
        default="little",
        help="Thứ tự byte (mặc định: little)"
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Hiển thị thêm Hex Dump chi tiết của mảng DATA"
    )

    args = parser.parse_args()
    limit = 9999999 if args.all else args.limit

    # Check if user specified a single .dat file directly
    if os.path.isfile(args.session) and args.session.endswith(".dat"):
        inspect_single_file(args.session, args.endian, limit, args.raw)
        return

    # Search for session zdata
    zdata_dir = find_latest_session_dir(args.session)
    if not zdata_dir:
        print(f"{Colors.RED}[LỖI]{Colors.RESET} Không tìm thấy thư mục 'zdata' nào trong đường dẫn: '{args.session}'")
        sys.exit(1)

    session_root = os.path.dirname(zdata_dir)

    print(f"\n{Colors.BOLD}{Colors.WHITE}═══════════════════════════════════════════════════════════════════════════════════════════════════════{Colors.RESET}")
    print(f"  {Colors.BOLD}{Colors.CYAN}RADAR TELEMETRY SESSION INSPECTOR{Colors.RESET}")
    print(f"  Thư mục Session : {Colors.YELLOW}{session_root}{Colors.RESET}")
    print(f"  Đường dẫn zdata : {Colors.DIM}{zdata_dir}{Colors.RESET}")
    print(f"  Chuẩn Endian    : {Colors.GREEN}{args.endian.upper()}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.WHITE}═══════════════════════════════════════════════════════════════════════════════════════════════════════{Colors.RESET}\n")

    topics = ["video", "hit", "ssr", "adsb"]
    if args.topic:
        topics = [args.topic]

    # Collect statistics and packets
    topic_data = {}
    all_timeline_packets = []
    total_packets_session = 0
    total_bytes_session = 0

    for t in ["video", "hit", "ssr", "adsb"]:
        t_dir = os.path.join(zdata_dir, t)
        files = sorted(glob.glob(os.path.join(t_dir, f"{t}_*.dat")))
        t_packets = []
        t_bytes = 0
        for f in files:
            size = os.path.getsize(f)
            t_bytes += size
            pkts = read_dat_file(f, args.endian)
            for p in pkts:
                p["topic"] = t
                p["file"] = os.path.basename(f)
            t_packets.extend(pkts)

        topic_data[t] = {
            "files": files,
            "packets": t_packets,
            "total_bytes": t_bytes,
            "valid_count": sum(1 for p in t_packets if p["is_valid"]),
            "corrupt_count": sum(1 for p in t_packets if not p["is_valid"])
        }
        total_packets_session += len(t_packets)
        total_bytes_session += t_bytes
        all_timeline_packets.extend(t_packets)

    # 1. Bảng tổng quan Overview
    print(f"{Colors.BOLD}[1] BẢNG TỔNG QUAN CÁC KÊNH (SESSION OVERVIEW):{Colors.RESET}")
    print(f"┌──────────┬──────────────┬──────────────┬───────────────┬────────────────────────────┐")
    print(f"│  KÊNH    │  SỐ FILE .DAT│  TỔNG GÓI    │  DUNG LƯỢNG   │ TRẠNG THÁI MARKER (SOP/EOP)│")
    print(f"├──────────┼──────────────┼──────────────┼───────────────┼────────────────────────────┤")

    for t in ["video", "hit", "ssr", "adsb"]:
        td = topic_data[t]
        t_color = TOPIC_COLORS.get(t, Colors.WHITE)
        num_files = len(td["files"])
        num_pkts = len(td["packets"])
        size_str = format_bytes(td["total_bytes"])
        if num_pkts == 0:
            status = f"{Colors.DIM}Không có dữ liệu           {Colors.RESET}"
        elif td["corrupt_count"] == 0:
            status = f"{Colors.GREEN}✓ Hợp lệ 100% (0x0D0D/0x0A0A){Colors.RESET}"
        else:
            err_text = f"✗ {td['corrupt_count']} gói lỗi!"
            status = f"{Colors.RED}{err_text:<26}{Colors.RESET}"

        print(f"│ {t_color}{t:<8}{Colors.RESET} │ {num_files:>12} │ {num_pkts:>12} │ {size_str:>13} │ {status} │")

    print(f"├──────────┼──────────────┼──────────────┼───────────────┼────────────────────────────┤")
    total_files = sum(len(td['files']) for td in topic_data.values())
    print(f"│ {Colors.BOLD}TỔNG CỘNG{Colors.RESET}│ {total_files:>12} │ {total_packets_session:>12} │ {format_bytes(total_bytes_session):>13} │                            │")
    print(f"└──────────┴──────────────┴──────────────┴───────────────┴────────────────────────────┘\n")

    if total_packets_session == 0:
        print(f"{Colors.YELLOW}Thông báo: Phiên ghi chưa có gói tin nào.{Colors.RESET}\n")
        return

    # 2. Chi tiết gói tin
    if args.timeline:
        # TIMELINE MODE
        all_timeline_packets.sort(key=lambda p: p["timestamp_us"])
        print(f"{Colors.BOLD}[2] DÒNG THỜI GIAN ĐỒNG BỘ TOÀN BỘ CÁC KÊNH (TIMELINE REPLAY ORDER):{Colors.RESET}")
        print(f"    (Hiển thị {min(len(all_timeline_packets), limit)} / {len(all_timeline_packets)} gói theo thứ tự thời gian thực)\n")
        print_packet_table(all_timeline_packets[:limit], show_topic=True, show_raw=args.raw)
    else:
        # TOPIC BY TOPIC MODE
        print(f"{Colors.BOLD}[2] CHI TIẾT GÓI TIN TỪNG KÊNH:{Colors.RESET}")
        for t in topics:
            td = topic_data[t]
            t_color = TOPIC_COLORS.get(t, Colors.WHITE)
            pkts = td["packets"]
            if not pkts:
                continue

            shown_pkts = pkts[:limit]
            print(f"\n───► Kênh: {t_color}{Colors.BOLD}{t.upper()}{Colors.RESET} ({len(pkts)} gói tin, hiển thị mẫu {len(shown_pkts)} gói đầu):")
            print_packet_table(shown_pkts, show_topic=False, show_raw=args.raw)


def print_packet_table(packets: List[Dict[str, Any]], show_topic: bool = False, show_raw: bool = False):
    """Vẽ bảng chi tiết các gói tin theo định dạng tabular trực quan, căn lề chuẩn xác từng ký tự."""
    if not packets:
        print("    (Không có dữ liệu)")
        return

    # Dynamic columns depending on whether Topic is shown
    top_hdr_top = "┬────────" if show_topic else ""
    top_hdr_bot = "┴────────" if show_topic else ""
    top_hdr     = "│  KÊNH  " if show_topic else ""
    top_div     = "┼────────" if show_topic else ""

    print(f"┌───────┬────────────────────────────{top_hdr_top}┬───────┬────────┬────────────────┬──────────┬───────────┬─────────────────────────┐")
    print(f"│  STT  │ THỜI GIAN NHẬN (UTC)       {top_hdr}│  SEQ  │ TRẠM ID│ TYPE / Ý NGHĨA │ PAYLOAD  │ MARKER OK │ PREVIEW DỮ LIỆU (HEX)   │")
    print(f"├───────┼────────────────────────────{top_div}┼───────┼────────┼────────────────┼──────────┼───────────┼─────────────────────────┤")

    for idx, p in enumerate(packets, 1):
        t_str = p["datetime_utc"]
        seq_val = f"{p['seq']:<5}"
        src_val = f"0x{p['src_id']:02X} ({p['src_id']})"
        type_hex = f"0x{p['type']:02X}"
        _, type_desc = TYPE_NAMES.get(p["type"], (p.get("topic", "?"), "Unknown"))
        type_val = f"{type_hex} {type_desc[:10]}"

        payload_val = f"{p['payload_len']} B"
        marker_text = "✓ Hợp lệ" if p["is_valid"] else "✗ Lỗi"
        marker_color = Colors.GREEN if p["is_valid"] else Colors.RED
        marker_padded = f"{marker_color}{marker_text:<9}{Colors.RESET}"

        hex_data = hex_preview(p["data_bytes"], max_len=8)

        top_val = ""
        if show_topic:
            top = p.get("topic", "")
            top_color = TOPIC_COLORS.get(top, Colors.WHITE)
            top_val = f"│ {top_color}{top:<6}{Colors.RESET} "

        print(f"│ {idx:>5} │ {Colors.DIM}{t_str}{Colors.RESET} {top_val}│ {Colors.CYAN}{seq_val}{Colors.RESET} │ {src_val:<6} │ {type_val:<14} │ {payload_val:>8} │ {marker_padded} │ {hex_data:<23} │")

        if show_raw and p["data_bytes"]:
            raw_full = " ".join(f"{b:02X}" for b in p["data_bytes"])
            print(f"│       └─► Raw Payload [{len(p['data_bytes'])}B]: {Colors.DIM}{raw_full}{Colors.RESET}")

    print(f"└───────┴────────────────────────────{top_hdr_bot}┴───────┴────────┴────────────────┴──────────┴───────────┴─────────────────────────┘")


def inspect_single_file(file_path: str, endian: str, limit: int, show_raw: bool):
    """Xem trực tiếp 1 file .dat cụ thể."""
    size = os.path.getsize(file_path)
    pkts = read_dat_file(file_path, endian)
    print(f"\n{Colors.BOLD}Chi tiết file: {Colors.CYAN}{file_path}{Colors.RESET}")
    print(f"Dung lượng: {format_bytes(size)} | Tổng số gói: {len(pkts)}")
    print_packet_table(pkts[:limit], show_topic=False, show_raw=show_raw)


if __name__ == "__main__":
    main()
