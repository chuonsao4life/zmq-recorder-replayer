#!/usr/bin/env python3
import argparse
import os
import signal
import struct
import sys
import time
import zmq

TYPE_TO_TOPIC = [
    (0x01, b"video", 1024),
    (0x02, b"hit",   128),
    (0x10, b"ssr",   256),
    (0x11, b"ssr",   256),
    (0xF1, b"adsb",  64),
]

COLORS = {
    "video": "\033[96m",
    "hit":   "\033[91m",
    "ssr":   "\033[93m",
    "adsb":  "\033[92m",
    "reset": "\033[0m",
}

SOP = 0x0D0D
EOP = 0x0A0A
running = True

def handle_sig(sig, frame):
    global running
    running = False

signal.signal(signal.SIGINT, handle_sig)
signal.signal(signal.SIGTERM, handle_sig)

def build_frame_rdp(seq: int, src_id: int, msg_type: int, payload_size: int) -> bytes:
    data = os.urandom(payload_size)
    core_header = struct.pack("<HBBH", seq & 0xFFFF, src_id & 0xFF, msg_type & 0xFF, payload_size & 0xFFFF)
    chksum = (sum(core_header) + sum(data)) & 0xFFFF
    header = struct.pack("<H", SOP) + core_header
    footer = struct.pack("<HH", chksum, EOP)
    return header + data + footer

def main():
    parser = argparse.ArgumentParser(description="Mock ZMQ Publisher + Live Terminal Printer")
    parser.add_argument("--bind", type=str, default="tcp://127.0.0.1:30004")
    parser.add_argument("--rate", type=float, default=10.0)
    args = parser.parse_args()

    ctx = zmq.Context()
    pub = ctx.socket(zmq.PUB)
    try:
        pub.bind(args.bind)
    except Exception as e:
        print(f"[!] Lỗi không thể bind vào {args.bind}: {e}", flush=True)
        sys.exit(1)

    print(f"[*] Đã mở cổng ZMQ PUB tại: {args.bind} (Tốc độ: {args.rate} gói/s)", flush=True)
    print("[*] Chờ 1000 ms cho Subscriber kết nối...", flush=True)
    time.sleep(1.0)

    print(f"\n{'THỜI GIAN':<12} | {'TOPIC':<7} | {'SEQ':<7} | {'TYPE':<6} | {'LEN':<7} | {'FRAME HEX (SOP ... EOP)'}", flush=True)
    print("-" * 82, flush=True)

    seq = 0
    interval = (1.0 / args.rate) if args.rate > 0 else 0.0

    while running:
        t0 = time.monotonic()
        msg_type, topic_bytes, payload_len = TYPE_TO_TOPIC[seq % len(TYPE_TO_TOPIC)]
        frame_rdp = build_frame_rdp(seq, 1, msg_type, payload_len)

        pub.send_multipart([topic_bytes, frame_rdp])

        topic_str = topic_bytes.decode("ascii")
        color = COLORS.get(topic_str, COLORS["reset"])
        rst = COLORS["reset"]
        ts = time.strftime("%H:%M:%S") + f".{int(time.time() * 1000) % 1000:03d}"
        hex_head = frame_rdp[:8].hex()
        hex_tail = frame_rdp[-4:].hex()

        print(
            f"{ts:<12} | {color}{topic_str:<7}{rst} | #{seq:<6} | 0x{msg_type:02X}   | {payload_len:<5} B | {hex_head} ... {hex_tail}",
            flush=True
        )

        seq += 1
        if interval > 0:
            rem = interval - (time.monotonic() - t0)
            if rem > 0:
                time.sleep(rem)

    print(f"\n[+] Đã dừng sau khi phát {seq} gói tin.", flush=True)
    pub.close()
    ctx.term()

if __name__ == "__main__":
    main()
