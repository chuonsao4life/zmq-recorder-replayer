# Technical Architecture & Engineering Guide: Multi-Topic Radar/Sensor Data Recording & Replay Engine Using C++20, ZeroMQ, and Qt6

---

## Table of Contents
1. [Big Picture & End-to-End Data Flow](#1-big-picture--end-to-end-data-flow)
   - [1.1 Domain Context: Aerospace & Defense Radar Surveillance](#11-domain-context-aerospace--defense-radar-surveillance)
   - [1.2 End-to-End Pipeline Architecture Diagram](#12-end-to-end-pipeline-architecture-diagram)
   - [1.3 Core Missions of the Recorder and Replay Subsystems](#13-core-missions-of-the-recorder-and-replay-subsystems)
2. [Detailed Input & Output Protocol Analysis](#2-detailed-input--output-protocol-analysis)
   - [2.1 Wire Network Protocol: ZeroMQ Multipart Message](#21-wire-network-protocol-zeromq-multipart-message)
   - [2.2 Binary Frame Specification: frame_RDP (Byte-by-Byte Layout)](#22-binary-frame-specification-frame_rdp-byte-by-byte-layout)
   - [2.3 On-Disk Storage Layout: .dat Files & Directory Hierarchy](#23-on-disk-storage-layout-dat-files--directory-hierarchy)
   - [2.4 Deep Dive into Computer Architecture: The Struct Padding Trap](#24-deep-dive-into-computer-architecture-the-struct-padding-trap)
   - [2.5 The Endianness Battlefield: Little-Endian vs Big-Endian](#25-the-endianness-battlefield-little-endian-vs-big-endian)
3. [Tech Stack Roles & Architectural Justification](#3-tech-stack-roles--architectural-justification)
   - [3.1 Modern C++20: High-Performance Systems Engineering](#31-modern-c20-high-performance-systems-engineering)
   - [3.2 ZeroMQ (libzmq / cppzmq): Breaking Beyond Raw TCP Sockets](#32-zeromq-libzmq--cppzmq-breaking-beyond-raw-tcp-sockets)
   - [3.3 Qt6 (QtCore): Cross-Platform Endianness, UTC Time, and File System](#33-qt6-qtcore-cross-platform-endianness-utc-time-and-file-system)
4. [Internal Ingestion & Storage Pipeline (Recorder Deep Dive)](#4-internal-ingestion--storage-pipeline-recorder-deep-dive)
   - [4.1 Receiver Thread: Socket Ingestion, UTC Microsecond Timestamping & Validation](#41-receiver-thread-socket-ingestion-utc-microsecond-timestamping--validation)
   - [4.2 BoundedByteQueue (256 MB): Drop-on-Overflow & Batch Swapping (std::swap)](#42-boundedbytequeue-256-mb-drop-on-overflow--batch-swapping-stdswap)
   - [4.3 Writer Thread & TopicWriter: 128 KB I/O Buffering, Lazy File Creation & 300s Segmentation](#43-writer-thread--topicwriter-128-kb-io-buffering-lazy-file-creation--300s-segmentation)
5. [Internal Playback & Transmission Pipeline (Replay Deep Dive)](#5-internal-playback--transmission-pipeline-replay-deep-dive)
   - [5.1 Sequential Segment Traversal via TopicReader](#51-sequential-segment-traversal-via-topicreader)
   - [5.2 Chronological Multi-Channel Merging with Min-Heap (N-Way Merge)](#52-chronological-multi-channel-merging-with-min-heap-n-way-merge)
   - [5.3 High-Precision Microsecond Playback Pacing (Hybrid Sleep + Spin-Wait)](#53-high-precision-microsecond-playback-pacing-hybrid-sleep--spin-wait)
6. [Implementation Blueprint from Scratch (Checklist & Runbook)](#6-implementation-blueprint-from-scratch-checklist--runbook)

---

## 1. Big Picture & End-to-End Data Flow

### 1.1 Domain Context: Aerospace & Defense Radar Surveillance
In modern air defense, maritime coastal surveillance, and civil Air Traffic Control / Air Traffic Management (ATC/ATM) centers, telemetry data streams continuously from multiple radar stations 24/7 with massive throughput and stringent real-time requirements.

Our system handles 4 specialized telemetry channels (categorized by ZeroMQ `topic`):
1. **`video` (Raw Radar Video / Digitized Video Sweep)**: High-frequency raw radar video return digitized directly from the primary receiver. This channel features the highest packet rate, representing electromagnetic reflections from terrain, precipitation, clutter, and aircraft skins.
2. **`hit` (Primary Radar Target Hit / Detection Plot)**: Discrete target detections produced after the primary radar's CFAR (Constant False Alarm Rate) adaptive thresholding engine. Each "hit" marks an energy spike suspected of being a moving airborne or surface reflector with specific range and azimuth attributes.
3. **`ssr` (Secondary Surveillance Radar / IFF Transponder Replies)**: Active cooperative interrogation and reply data operating on 1030/1090 MHz between ground interrogators and onboard transponders. It carries squawk identity codes (Mode 3/A), barometric pressure altitude (Mode C), Mode S selective address, or military IFF (Identification Friend or Foe).
4. **`adsb` (Automatic Dependent Surveillance - Broadcast)**: Unsolicited periodic 1090 MHz Extended Squitter broadcasts where modern aircraft broadcast their high-integrity GNSS satellite coordinates, geometric altitude, ground speed, heading, and flight callsign.

#### Why is High-Performance Recording & Replay Crucial?
- **Operational Cost Optimization**: Radiating a multi-megawatt radar transmitter consumes substantial electric power and subjects costly high-power traveling-wave tubes (TWT) or magnetrons to physical wear.
- **Operator & Combat Crew Training**: Realistic simulated radar feeds (e.g., dense air traffic, severe storm clutter, electronic counter-measures) can be replayed to console operators repeatedly without dispatching live aircraft.
- **Incident Investigation & Algorithm Refinement**: When investigating near-misses, tracking filter divergences, or false-track anomalies (e.g., in multi-sensor fusion or Kalman filters), algorithmic engineers require an **exact byte-for-byte replica** of historical telemetry replayed with **microsecond-accurate relative timing**.

---

### 1.2 End-to-End Pipeline Architecture Diagram

The system operates as a message-driven distributed pipeline centered around two high-performance C++20 daemons: the **Recorder** and the **Replay** engine.

```
 +-------------------------------------------------------------------------------+
 |                       REAL OR SIMULATED TELEMETRY SOURCE                      |
 |  [appmrepl] Replays recorded raw radar files (.rdata) -> Raw TCP Port 8889    |
 +-------------------------------------------------------------------------------+
                                         |
                                         | Raw TCP Byte Stream
                                         v
 +-------------------------------------------------------------------------------+
 |                              (1) ZMQ BRIDGE                                   |
 | - Connects over TCP to 127.0.0.1:8889 (Auto-reconnects on disconnection).     |
 | - Splits raw byte stream into discrete [frame_RDP] units.                     |
 | - Reads TYPE byte -> Maps to topic: "video", "hit", "ssr", "adsb".            |
 | - Emits ZeroMQ Multipart Message: [Part 1: Topic][Part 2: frame_RDP].         |
 | - Binds ZMQ PUB Socket: tcp://192.168.5.21:30003                              |
 +-------------------------------------------------------------------------------+
                                         |
                                         | Local High-Speed LAN
                                         | ZeroMQ PUB / SUB Multipart Stream
                                         v
 +-------------------------------------------------------------------------------+
 |                              (2) RECORDER ENGINE                              |
 | - Connects ZMQ SUB Socket to tcp://192.168.5.21:30003                         |
 | - Subscribes to 4 topics: "video", "hit", "ssr", "adsb".                      |
 | - Ingests message, stamps UTC microsecond arrival time.                       |
 | - Validates SOP (0x0D0D) and EOP (0x0A0A) frame boundaries.                   |
 | - Pushes into non-blocking memory queue (BoundedByteQueue <= 256 MB).         |
 | - Writer thread drains batches and rolls segment files every 300 seconds.     |
 | - Lazy Creation: Directories and files created only upon first valid packet.  |
 +-------------------------------------------------------------------------------+
                                         |
                                         | High-Throughput NVMe / SSD File I/O
                                         v
 +-------------------------------------------------------------------------------+
 |                           ON-DISK RECORDED STORAGE                            |
 |                                                                               |
 |  <out_dir>/                                                                   |
 |  └── 20260925_0926/                  <-- Session Root (Session Start UTC)     |
 |      └── zdata/                                                               |
 |          ├── video/video_20260925_092600.dat  <-- 300s Segment 0 (09:26:00)   |
 |          ├── hit/hit_20260925_092600.dat                                      |
 |          ├── ssr/ssr_20260925_092600.dat                                      |
 |          └── adsb/adsb_20260925_092600.dat                                    |
 +-------------------------------------------------------------------------------+
                                         |
                                         | Sequential Read via 128 KB Buffers
                                         v
 +-------------------------------------------------------------------------------+
 |                               (3) REPLAY ENGINE                               |
 | - Spawns a dedicated TopicReader for each topic subfolder.                    |
 | - Employs a Min-Heap (std::priority_queue) to merge 4 streams chronologically.|
 | - Binds ZMQ PUB Socket: tcp://192.168.5.31:30004                              |
 | - Slow-Joiner Mitigation: Sleeps 1000 ms to allow subscriber handshakes.      |
 | - Microsecond Hybrid Pacing: OS coarse sleep + fine-grained CPU _mm_pause().  |
 | - Transmits 2-part messages [topic][frame_RDP] with wire-exact precision.     |
 +-------------------------------------------------------------------------------+
                                         |
                                         | tcp://192.168.5.31:30004
                                         v
 +-------------------------------------------------------------------------------+
 |                          DOWNSTREAM SUBSCRIBERS                               |
 | - Tactical Air Defense PPI & Situation Displays.                              |
 | - Multi-Radar Tracking & Data Fusion Engines (Kalman / IMM Filters).          |
 | - Command & Control (C2) Audit & Quality Assurance Terminals.                 |
 +-------------------------------------------------------------------------------+
```

---

### 1.3 Core Missions of the Recorder and Replay Subsystems
- **Recorder Subsystem**: Must sustain zero packet loss under sustained high burst rates (tens of thousands of packets/sec). It must never block the socket receiving thread, must protect host memory from out-of-memory (OOM) crashes via backpressure drops, and must persist data in an aligned, portable, byte-exact binary format.
- **Replay Subsystem**: Must guarantee **absolute temporal and binary fidelity**. The relative inter-packet arrival time must be reproduced down to the microsecond. Simultaneously, replayed payloads must match the original recordings **byte-for-byte (100% exact match)** without corruption or ordering inversions.

---

## 2. Detailed Input & Output Protocol Analysis

For a systems developer, mastering every single bit and byte moving across the network interface card (NIC) and storage controller is mandatory.

### 2.1 Wire Network Protocol: ZeroMQ Multipart Message
Across the network link between ZMQ Bridge $\rightarrow$ Recorder (`tcp://192.168.5.21:30003`) and Replay $\rightarrow$ Subscribers (`tcp://192.168.5.31:30004`), data is transmitted as a **Two-Part ZeroMQ Multipart Message**:

```
+-------------------------------------------------------------------+
|                   ZEROMQ MULTIPART MESSAGE                        |
|                                                                   |
|   +--------------------------+   +----------------------------+   |
|   |  Part 1: Topic (ASCII)   |   |   Part 2: frame_RDP (Raw)  |   |
|   |  "video", "hit", ...     |   |   Raw Binary RDP Payload   |   |
|   +--------------------------+   +----------------------------+   |
|      (Flag: ZMQ_SNDMORE)             (Flag: None / 0)             |
+-------------------------------------------------------------------+
```

- **Part 1 (Topic String)**: A null-terminated or sized ASCII string representing the channel identifier (`"video"`, `"hit"`, `"ssr"`, `"adsb"`). ZeroMQ inspects this part in kernel/transport space to execute prefix filtering for subscribers.
- **Part 2 (frame_RDP)**: The raw, unmodified binary radar frame. It is transmitted as a single contiguous binary blob with no structural alteration.

---

### 2.2 Binary Frame Specification: `frame_RDP` (Byte-by-Byte Layout)
The `frame_RDP` (Radar Data Protocol frame) encapsulates raw sensor information. Every valid frame has a minimum size of **12 bytes** (when the payload `DATA` field is 0 bytes long).

The exact memory layout is defined below:

```
Byte Offset:
 0       1       2       3       4       5       6       7       8 ... 8+LEN-1  8+LEN   9+LEN   10+LEN  11+LEN
+-------+-------+-------+-------+-------+-------+-------+-------+-------------+-------+-------+-------+-------+
|  SOP (0x0D0D) |  SEQ (u16)    |SRC_ID | TYPE  |  LEN (u16)    | DATA (bytes)| CHKSUM (u16)  |  EOP (0x0A0A) |
+-------+-------+-------+-------+-------+-------+-------+-------+-------------+-------+-------+-------+-------+
|<--- 2 Bytes ->|<--- 2 Bytes ->|<-1 B->|<-1 B->|<--- 2 Bytes ->|<- LEN Bytes>|<--- 2 Bytes ->|<--- 2 Bytes ->|
```

#### Detailed Field Specification:
| Field Name | Data Type | Size (Bytes) | Example / Constraints | Description & Semantics |
| :--- | :--- | :--- | :--- | :--- |
| **SOP** | `uint16_t` | 2 | `0x0D0D` | **Start of Packet**: Fixed synchronization preamble identifying frame onset. |
| **SEQ** | `uint16_t` | 2 | `0x0001`, `0x0002`... | **Sequence Counter**: Monotonically incrementing packet counter to track loss. |
| **SRC_ID**| `uint8_t` | 1 | `0x21` | **Source Identifier**: Radar station ID or sensor transceiver index. |
| **TYPE** | `uint8_t` | 1 | `0x01`, `0x02`, `0x10`... | **Message Type**: Telemetry payload category, maps directly to a topic. |
| **LEN** | `uint16_t` | 2 | `0x0080` (128 bytes) | **Data Length**: Exact byte count of the subsequent `DATA` field. |
| **DATA** | `uint8_t[]` | `LEN` | Byte array | Sensor-specific business payload (plots, sweeps, targets). |
| **CHKSUM**| `uint16_t` | 2 | 16-bit arithmetic sum | Integrity checksum covering preamble through payload. |
| **EOP** | `uint16_t` | 2 | `0x0A0A` | **End of Packet**: Fixed synchronization postamble marking frame termination. |

#### Mapping Rule: `TYPE` Byte to Topic String
The ZMQ Bridge maps incoming RDP types to logical ZeroMQ topics using the following deterministic specification:
- `TYPE = 0x01` $\longrightarrow$ Mapped to topic **`video`**
- `TYPE = 0x02` $\longrightarrow$ Mapped to topic **`hit`**
- `TYPE = 0x10` or `0x11` $\longrightarrow$ Mapped to topic **`ssr`**
- `TYPE = 0xF1` $\longrightarrow$ Mapped to topic **`adsb`**

In [`EndianCodec.h`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/common/EndianCodec.h#L33-L45), the inline function `verifyFrameMarkers` checks frame validity:
```cpp
inline bool verifyFrameMarkers(const uint8_t* data, size_t len, bool useBigEndian) {
    if (len < 12) return false;  // Minimum frame size must be at least 12 bytes
    uint16_t sop = useBigEndian ? qFromBigEndian<uint16_t>(data) : qFromLittleEndian<uint16_t>(data);
    uint16_t eop = useBigEndian ? qFromBigEndian<uint16_t>(data + len - 2) : qFromLittleEndian<uint16_t>(data + len - 2);
    return (sop == SOP_MARKER) && (eop == EOP_MARKER);
}
```

---

### 2.3 On-Disk Storage Layout: `.dat` Files & Directory Hierarchy

#### Directory Tree Structure:
Recorded sessions are structured hierarchically based on UTC wall-clock time:
```
<out_dir>/
└── yyyyMMdd_HHmm/                     <-- Session Root (Recording Inception Time)
    └── zdata/
        ├── video/
        │   ├── video_20260925_092600.dat  <-- Segment 0 (09:26:00 -> 09:31:00 UTC)
        │   └── video_20260925_093100.dat  <-- Segment 1 (09:31:00 -> 09:36:00 UTC)
        ├── hit/
        │   └── hit_20260925_092600.dat
        ├── ssr/
        │   └── ssr_20260925_092600.dat
        └── adsb/
            └── adsb_20260925_092600.dat
```
> [!IMPORTANT]
> **Timestamp Format Conventions**:
> - **Session Root Folder**: Uses `yyyyMMdd_HHmm` (e.g., `20260925_0926` - UTC year, month, day, hour, minute). Formatted by [`formatSessionTimestamp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/common/EndianCodec.h#L47-L51).
> - **Segment File Names**: Uses `<topic>_yyyyMMdd_HHmmss.dat` (e.g., `video_20260925_092600.dat` - UTC year, month, day, hour, minute, second representing the 300-second window origin). Formatted by [`formatSegmentTimestamp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/common/EndianCodec.h#L53-L57).

#### On-Disk Packet Architecture: The 10-Byte Header Protocol
Within each `.dat` file, packets are serialized sequentially back-to-back (**no padding, no delimiter gaps**). The topic string is **not stored** inside individual packets because it is already encoded by the directory path and filename.

Every recorded packet consists of an **exact 10-byte fixed header** followed immediately by the variable-length `frame_RDP`:

```
+----------------------------------------------------------------------------------------+
|                          ON-DISK PACKET LAYOUT (.DAT FILE)                             |
|                                                                                        |
|  0               7 8         9 10                                            10+data_len|
| +-----------------+-----------+--------------------------------------------------------+|
| |  time_stamp u64 | data_len  |                      frame_RDP                         ||
| |  (8 Bytes)      | (2 Bytes) |                  (data_len Bytes)                      ||
| +-----------------+-----------+--------------------------------------------------------+|
| |<-- 10-Byte Fixed Header --->|<---------------- Variable Payload -------------------->||
+----------------------------------------------------------------------------------------+
```

- **`time_stamp` (8 Bytes - `uint64_t`)**: Ingestion timestamp in microseconds since Unix Epoch (`1970-01-01 00:00:00 UTC`), stamped immediately upon packet arrival at the network socket.
- **`data_len` (2 Bytes - `uint16_t`)**: Byte length of the following `frame_RDP`.
- **`frame_RDP` (`data_len` Bytes)**: Raw radar frame bytes spanning from `SOP 0x0D0D` to `EOP 0x0A0A`.

Total byte footprint per packet on disk = `10 + data_len` bytes.

---

### 2.4 Deep Dive into Computer Architecture: The Struct Padding Trap

A frequent mistake made by intermediate C++ developers is attempting to serialize headers using raw C++ structs:
```cpp
// !!! DANGEROUS CODE - DO NOT WRITE THIS IN LOW-LATENCY STORAGE ENGINES !!!
struct DiskHeader {
    uint64_t time_stamp; // 8 bytes
    uint16_t data_len;   // 2 bytes
}; // You might assume sizeof(DiskHeader) == 10 bytes?

DiskHeader hdr{ts, len};
fwrite(&hdr, sizeof(hdr), 1, fp); // CATASTROPHE OCCURS HERE!
```

#### Why Does This Break Binary Storage?
Modern 64-bit microprocessors (x86_64, ARM64) require or heavily favor **Natural Data Alignment**. A 64-bit integer (`uint64_t`) must be stored at a memory address divisible by 8 to prevent split bus cycles or hardware alignment faults.
When the compiler lays out `DiskHeader`:
- `time_stamp` takes 8 bytes (offset 0 to 7).
- `data_len` takes 2 bytes (offset 8 to 9).
- To align subsequent elements or array allocations to an 8-byte boundary, the compiler automatically injects **6 invisible padding bytes**:
  $$8 \text{ bytes (timestamp)} + 2 \text{ bytes (data\_len)} + \mathbf{6 \text{ bytes (compiler padding)}} = \mathbf{16 \text{ bytes}}!$$

```
Memory Layout of Uncontrolled C++ Struct:
[Byte 0 .. Byte 7] : time_stamp (8B)
[Byte 8 .. Byte 9] : data_len   (2B)
[Byte 10.. Byte 15]: GARBAGE PADDING (6B) <--- Injected silently by GCC/Clang/MSVC!
```

Executing `fwrite(&hdr, sizeof(hdr), 1, fp)` commits 6 garbage bytes to disk. When the Replay engine subsequently attempts to deserialize 10 bytes, it consumes 6 bytes of padding as the start of `frame_RDP`, hopelessly desynchronizing all subsequent packet offsets, destroying `SOP` markers, and corrupting the dataset.

#### Production Solution Implemented in this Project:
We **never dump raw structs to disk**. Instead, in [`TopicWriter.cpp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/TopicWriter.cpp#L60-L65) and [`EndianCodec.h`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/common/EndianCodec.h#L13-L21), we manually pack an explicit byte buffer `uint8_t hdr[10]`:
```cpp
inline void serializeHeader(uint64_t timestamp_us, uint16_t data_len, uint8_t* dest, bool useBigEndian) {
    if (useBigEndian) {
        qToBigEndian(timestamp_us, dest);       // Serializes 8 bytes into dest[0..7]
        qToBigEndian(data_len, dest + 8);       // Serializes 2 bytes into dest[8..9]
    } else {
        qToLittleEndian(timestamp_us, dest);    // Serializes 8 bytes into dest[0..7]
        qToLittleEndian(data_len, dest + 8);    // Serializes 2 bytes into dest[8..9]
    }
}
```
This guarantees that the on-disk header footprint is **exactly 10 bytes**, independent of compiler versions, compiler flags, optimization levels, or CPU architecture.

---

### 2.5 The Endianness Battlefield: Little-Endian vs Big-Endian

Endianness dictates the order in which individual bytes of a multi-byte integer are arranged in computer memory:
- **Little-Endian (LE)**: The least significant byte (LSB) is stored at the lowest memory address. This is standard on commodity consumer and server hardware (Intel x86, AMD64, Apple Silicon).
- **Big-Endian (BE)**: The most significant byte (MSB) is stored at the lowest memory address. This is the official **Network Byte Order** and the native format of older avionics, radar compute units, and architectures like PowerPC and SPARC.

#### Concrete Memory Comparison:
Suppose `time_stamp = 0x0102030405060708` (8 bytes) and `data_len = 0x0080` (128 bytes, 2 bytes):

```
Memory Offset:      [0]   [1]   [2]   [3]   [4]   [5]   [6]   [7]   [8]   [9]
--------------------------------------------------------------------------------
Little-Endian:      0x08  0x07  0x06  0x05  0x04  0x03  0x02  0x01  0x80  0x00
(Reversed bytes)   |<------- time_stamp (LSB First) ------->|  |<data_len>|

Big-Endian:         0x01  0x02  0x03  0x04  0x05  0x06  0x07  0x08  0x00  0x80
(Human order)      |<------- time_stamp (MSB First) ------->|  |<data_len>|
```

Our system supports both modes seamlessly via the runtime flag `--endian=little` or `--endian=big`. Leveraging Qt's `<QtEndian>` templates, the compiler optimizes byte swapping into single hardware instructions (`bswap` on x86_64), achieving sub-nanosecond translation.

---

## 3. Tech Stack Roles & Architectural Justification

Each component of the software stack is purposefully chosen to address specific systems engineering challenges.

### 3.1 Modern C++20: High-Performance Systems Engineering

| C++20 Feature / Primitives | Key Source Files | Technical Purpose & Value |
| :--- | :--- | :--- |
| **`std::thread` & `std::mutex`** | [`RecorderEngine.cpp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/RecorderEngine.cpp#L43-L44) | Completely decouples network socket ingestion (`ReceiverThread`) from storage I/O (`WriterThread`), ensuring disk stalls never drop incoming network packets. |
| **`std::condition_variable`** | [`BoundedByteQueue.h`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/BoundedByteQueue.h#L51-L54) | Suspends the Writer thread in kernel sleep when the queue is dry, waking it instantly upon incoming telemetry without 100% CPU spinning. |
| **`std::atomic` (Memory Ordering)** | [`BoundedByteQueue.h`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/BoundedByteQueue.h#L66) | Manages `stop_requested_` using `std::memory_order_release` and `std::memory_order_acquire` for lockless cross-core visibility without heavy mutex overhead. |
| **Zero-Copy & `std::move`** | [`BoundedByteQueue.h`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/BoundedByteQueue.h#L38) | Transfers ownership of payload buffers across thread boundaries via Rvalue references, avoiding multi-megabyte `memcpy` operations. |
| **`std::priority_queue` (Min-Heap)**| [`ReplayEngine.cpp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/replay/ReplayEngine.cpp#L73) | Merges $N$ independent chronological telemetry streams with optimal $O(\log N)$ algorithmic complexity. |
| **`std::chrono::system_clock`** | [`Types.h`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/common/Types.h#L33-L36) | Queries real-world UTC epoch time down to the microsecond for packet recording stamps. |
| **`std::chrono::steady_clock`** | [`ReplayEngine.cpp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/replay/ReplayEngine.cpp#L90) | High-resolution monotonic clock guaranteed never to jump backwards during NTP clock adjustments, forming the rock-solid foundation for playback pacing. |

---

### 3.2 ZeroMQ (`libzmq` / `cppzmq`): Breaking Beyond Raw TCP Sockets

Developers often ask: *"Why incorporate ZeroMQ instead of writing standard OS TCP Berkeley sockets?"*

The table below contrasts the fundamental engineering trade-offs:

```
+------------------------------------+------------------------------------+
|          RAW TCP SOCKETS           |             ZEROMQ                 |
+------------------------------------+------------------------------------+
| 1. Unstructured Byte Stream:       | 1. Message-Oriented Framing:       |
| TCP treats data as an endless byte | ZeroMQ guarantees atomic message   |
| stream. Packets can coalesce       | boundaries. You receive the exact  |
| (Nagle's algorithm) or fragment.   | byte message that was sent, with   |
| You must write complex delimiter/  | zero application-level framing     |
| length-prefix parsing loops.       | overhead.                          |
+------------------------------------+------------------------------------+
| 2. Point-to-Point Coupling:        | 2. Flexible PUB/SUB Abstraction:   |
| You must manually manage accept,   | Decouples publishers from clients. |
| listen, connection state, and write| Handles reconnects automatically   |
| custom reconnect logic on dropouts.| during network disruptions.        |
+------------------------------------+------------------------------------+
| 3. Application-Level Filtering:    | 3. Kernel/Transport Topic Filters: |
| The application must ingest every  | ZMQ SUB applies topic filtering    |
| packet across all topics before    | before memory allocation, dropping |
| discarding unwanted data.          | unneeded data early.               |
+------------------------------------+------------------------------------+
```

#### Two Critical ZeroMQ Architectural Capabilities Used:

1. **Automatic In-Transport Topic Filtering**:
   In [`RecorderEngine.cpp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/RecorderEngine.cpp#L75-L77), the Recorder subscribes only to configured topics:
   ```cpp
   for (const auto& topic : config_.topics) {
       sub_sock.set(zmq::sockopt::subscribe, topic);
   }
   ```
   If the bridge introduces an unexpected topic, ZeroMQ filters it out in C-core space before it ever enters C++ application memory.

2. **The "Slow Joiner" Syndrome & The 1000ms Mitigation**:
   In ZeroMQ PUB/SUB sockets over `tcp://`, PUB sockets do not wait for subscribers. Establishing an asynchronous TCP connection and completing the internal ZeroMQ subscription handshake takes several milliseconds.
   If a publisher binds and immediately starts sending, **the first several hundred packets are lost forever into a black hole**.
   To mitigate this, [`ReplayEngine.cpp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/replay/ReplayEngine.cpp#L68-L70) enforces a mandatory setup pause:
   ```cpp
   std::cout << "[ReplayEngine] Waiting " << config_.slow_joiner_ms
             << " ms for subscribers to connect (slow-joiner mitigation)..." << std::endl;
   std::this_thread::sleep_for(std::chrono::milliseconds(config_.slow_joiner_ms));
   ```
   This 1000 ms pause guarantees all network subscribers have completed their handshakes before the first telemetry packet is emitted.

---

### 3.3 Qt6 (`QtCore`): Cross-Platform Endianness, UTC Time, and File System
Instead of relying on fragmented platform-specific APIs:
- **`<QtEndian>`**: Provides `qToLittleEndian`, `qToBigEndian`, `qFromLittleEndian`, and `qFromBigEndian`. These are type-safe template functions that map directly to single-cycle hardware instructions on both Windows and Linux.
- **`QDateTime` & `QTimeZone::UTC`**: Enforces strict UTC time formatting (`yyyyMMdd_HHmm`), preventing timezone offsets or Daylight Saving Time (DST) changes from corrupting folder structures.
- **`QDir::mkpath`**: Robust recursive directory creation (equivalent to `mkdir -p`), seamlessly resolving path separators (`/` vs `\`) across platforms.

---

## 4. Internal Ingestion & Storage Pipeline (Recorder Deep Dive)

The Recorder subsystem implements an asynchronous, thread-isolated **Producer-Consumer** architecture:

```
                  RECORDER SUBSYSTEM INTERNAL ARCHITECTURE
                    
 [ZMQ SUB Socket] (tcp://192.168.5.21:30003)
        |
        v
 +-------------------------------------------------------+
 |               THREAD 1: RECEIVER THREAD               |
 | 1. sub_sock.recv(topic) & recv(payload)               |
 | 2. Time-stamp arrival: currentUtcMicroseconds()       |
 | 3. Marker integrity check: verifyFrameMarkers()       |
 | 4. Encapsulate in TelemetryMessage struct             |
 +-------------------------------------------------------+
        |
        | push(std::move(msg))
        v
 +-------------------------------------------------------+
 |                 BOUNDED BYTE QUEUE                    |
 | - Memory limit: max_bytes = 256 MB                    |
 | - Drop-on-Overflow policy (Drop new if queue full)    |
 | - Thread synchronization: mutex + condition_variable  |
 +-------------------------------------------------------+
        |
        | drain(batch) via ultra-fast std::swap
        v
 +-------------------------------------------------------+
 |                THREAD 2: WRITER THREAD                |
 | 1. Iterate through drained local batch.               |
 | 2. Calculate segment index: seg_idx = delta / 300s.   |
 | 3. Roll segment if boundary crossed.                  |
 | 4. Lookup or instantiate TopicWriter.                 |
 | 5. Serialize 10-byte header and write to 128KB buffer.|
 +-------------------------------------------------------+
        |
        +----------> [video_*.dat] (128 KB setvbuf)
        +----------> [hit_*.dat]   (128 KB setvbuf)
        +----------> [ssr_*.dat]   (128 KB setvbuf)
        +----------> [adsb_*.dat]  (128 KB setvbuf)
```

### 4.1 Receiver Thread: Socket Ingestion, UTC Microsecond Timestamping & Validation
Implemented in [`RecorderEngine::receiverLoop`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/RecorderEngine.cpp#L67-L138):

1. **Socket Configuration & Receive Timeout**:
   - Configures `rcvtimeo = 200` ms. When no telemetry arrives for 200ms, `sub_sock.recv()` unblocks and returns `false`. This allows the receiver loop to check `stop_requested_` and terminate cleanly during `SIGINT` (`Ctrl+C`).
2. **Immediate UTC Timestamping**:
   - Immediately upon receiving the payload part, `currentUtcMicroseconds()` is captured. This ensures the timestamp represents true network arrival time rather than post-queue processing time.
3. **Preamble/Postamble Verification**:
   - Calls `verifyFrameMarkers(payload_ptr, payload_len, config_.use_big_endian)`.
   - If `len < 12`, `SOP != 0x0D0D`, or `EOP != 0x0A0A`, the frame is counted as corrupted (`total_invalid_++`) and dropped immediately.
4. **Zero-Copy Ingestion**:
   - Populates a [`TelemetryMessage`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/common/Types.h#L15-L24) and calls `queue_->push(std::move(msg))`, transferring vector memory without duplication.

---

### 4.2 BoundedByteQueue (256 MB): Drop-on-Overflow & Batch Swapping (`std::swap`)
Defined in [`BoundedByteQueue.h`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/BoundedByteQueue.h), this queue is the core stability anchor under heavy load.

#### 1. Drop-on-Overflow Policy
If storage I/O slows down (e.g., during disk controller write flushing), telemetry queues can consume excessive RAM, causing system Out-of-Memory (OOM) crashes.
- The queue tracks its exact heap footprint in bytes via `current_bytes_`.
- If enqueuing a new message would exceed 256 MB (`max_bytes_`):
  ```cpp
  if (current_bytes_ + msg_size > max_bytes_) {
      dropped_messages_.fetch_add(1, std::memory_order_relaxed);
      dropped_bytes_.fetch_add(msg_size, std::memory_order_relaxed);
      return false; // DROP IMMEDIATELY!
  }
  ```
- **Golden Rule**: **Never block the receiver thread**. Dropping the newest incoming packet and recording telemetry metrics is far better than stalling the network thread and triggering kernel socket buffer overflows.

#### 2. Batch Swapping via `std::swap`
Locking a mutex for every individual packet causes severe **Lock Contention** between Receiver and Writer threads.
Our engine employs pointer batch swapping via [`std::swap`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/BoundedByteQueue.h#L57):
```cpp
bool drain(std::deque<TelemetryMessage>& dest, std::chrono::milliseconds timeout) {
    dest.clear();
    std::unique_lock<std::mutex> lock(mutex_);
    if (queue_.empty() && !stop_requested_) {
        cv_.wait_for(lock, timeout, [this]() { return !queue_.empty() || stop_requested_; });
    }

    if (!queue_.empty()) {
        std::swap(queue_, dest); // EXECUTED IN NANOSECONDS!
        current_bytes_ = 0;
        return true;
    }
    return false;
}
```
After `std::swap` executes, thousands of queued messages are transferred into the Writer's local `dest` container. The mutex is released immediately, allowing the Writer thread to flush to disk while the Receiver thread continues enqueuing without delay.

---

### 4.3 Writer Thread & `TopicWriter`: 128 KB I/O Buffering, Lazy File Creation & 300s Segmentation

Implemented in [`RecorderEngine::writerLoop`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/RecorderEngine.cpp#L140-L183) and [`TopicWriter.cpp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/TopicWriter.cpp):

#### 1. User-Space 128 KB Buffering (`setvbuf`):
Issuing a kernel syscall (`write()`) for every small radar packet causes substantial CPU context-switching overhead.
`TopicWriter` assigns a dedicated 128 KB buffer to each open file descriptor:
```cpp
constexpr size_t IO_BUFFER_SIZE = 128 * 1024; // 128 KB
std::vector<char> io_buffer_(IO_BUFFER_SIZE);
std::setvbuf(file_, io_buffer_.data(), _IOFBF, io_buffer_.size());
```
Full buffering (`_IOFBF`) instructs the C runtime library to flush to disk only when 128 KB of binary data has accumulated, maximizing NVMe sequential write efficiency.

#### 2. Lazy File Creation (No Empty 0-Byte Files):
If an auxiliary radar channel (e.g., SSR or ADSB) is inactive for hours, pre-creating segment files would produce thousands of empty 0-byte files.
- `rollSegment()` simply closes the active file and sets `file_ = nullptr`.
- A topic's segment directory and `.dat` file are created by `ensureFileOpen()` **only when the first valid telemetry packet for that topic arrives** within that 300s segment window.

#### 3. 300-Second Segmentation Rollover Algorithm:
As packets are processed, their segment index is determined relative to the session start timestamp:
```cpp
uint64_t segment_duration_us = static_cast<uint64_t>(config_.segment_sec) * 1000000ULL; // 300,000,000 us
uint64_t seg_idx = (msg.timestamp_us >= session_start_us_)
    ? ((msg.timestamp_us - session_start_us_) / segment_duration_us)
    : 0;
uint64_t seg_start_ts = session_start_us_ + seg_idx * segment_duration_us;

if (seg_idx != current_segment_idx) {
    for (auto& [_, writer] : writers_) {
        writer->rollSegment(seg_start_ts); // Close preceding segment files
    }
    current_segment_idx = seg_idx;
}
```
Integer division against microsecond timestamps guarantees rigid 300-second alignment without cumulative timing drift.

---

## 5. Internal Playback & Transmission Pipeline (Replay Deep Dive)

The Replay subsystem addresses a complex challenge: **How do we reconstruct an exact chronological stream across 4 independent telemetry channels recorded into separate files?**

```
                 REPLAY SUBSYSTEM INTERNAL ARCHITECTURE
                 
  video/video_*.dat    hit/hit_*.dat      ssr/ssr_*.dat      adsb/adsb_*.dat
         |                  |                  |                  |
         v                  v                  v                  v
   [TopicReader 0]    [TopicReader 1]    [TopicReader 2]    [TopicReader 3]
   (128 KB Buffer)    (128 KB Buffer)    (128 KB Buffer)    (128 KB Buffer)
         \                  \                  /                  /
          \                  \                /                  /
           v                  v              v                  v
     +---------------------------------------------------------------+
     |                   MIN-HEAP (std::priority_queue)              |
     |  Maintains exactly 1 candidate packet per active reader:      |
     |  HeapItem { msg, reader_idx } sorted by msg.timestamp_us      |
     +---------------------------------------------------------------+
                                     |
                                     | 1. Pop packet with minimum timestamp
                                     v
     +---------------------------------------------------------------+
     |              HIGH-PRECISION HYBRID PACING ENGINE              |
     | - delta_us = item.timestamp_us - session_start_ts             |
     | - target_time = real_start + delta_us                         |
     | - If gap > 1500us: std::this_thread::sleep_until(...)         |
     | - In final 1500us: Spin-wait with CPU _mm_pause()             |
     +---------------------------------------------------------------+
                                     |
                                     | 2. Emit 2-part message to ZMQ PUB
                                     v
     +---------------------------------------------------------------+
     |        ZMQ PUB SOCKET (tcp://192.168.5.31:30004)              |
     |        pub_sock.send([topic], ZMQ_SNDMORE);                   |
     |        pub_sock.send([frame_RDP], 0);                         |
     +---------------------------------------------------------------+
                                     |
                                     | 3. Replenish heap from the same reader
                                     v
                     readers[item.reader_idx]->getNext()
```

### 5.1 Sequential Segment Traversal via `TopicReader`
Implemented in [`TopicReader.cpp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/replay/TopicReader.cpp):
1. **Chronological File Discovery**:
   Discovers files matching `<topic>_*.dat` using `QDir::entryList` sorted by `QDir::Name`. Because filenames use `yyyyMMdd_HHmmss`, alphabetical sorting matches chronological recording order.
2. **Seamless Multi-Segment Rollover**:
   In `getNext()`:
   - Reads the 10-byte header from the current segment file.
   - Upon encountering EOF, it automatically closes the completed file, opens the next segment in the sequence, and resumes reading without interrupting playback.
   - Robustly handles corrupted or truncated trailing frames.

---

### 5.2 Chronological Multi-Channel Merging with Min-Heap ($N$-Way Merge)
Loading gigabytes of data into RAM to sort globally would exhaust system memory. The Replay engine applies the classical **$K$-Way Merge algorithm using a Min-Heap**, maintaining constant, minimal RAM utilization.

Defined in [`ReplayEngine.h`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/replay/ReplayEngine.h#L32-L39):
```cpp
struct HeapItem {
    TelemetryMessage msg;
    size_t reader_idx{0};

    // Greater-than operator creates a Min-Heap (smallest timestamp at the top)
    bool operator>(const HeapItem& other) const noexcept {
        return msg.timestamp_us > other.msg.timestamp_us;
    }
};
```

#### Step-by-Step Heap Execution Trace:
Suppose 4 active readers yield the following timestamps (in seconds):
- Reader 0 (`video`): $10.002, 10.008, 10.012\dots$
- Reader 1 (`hit`): $10.001, 10.005, 10.009\dots$
- Reader 2 (`ssr`): $10.003, 10.007, 10.011\dots$
- Reader 3 (`adsb`): $10.150, 10.300, 10.450\dots$

```
STEP 1: PRIMING THE HEAP
Each reader reads exactly 1 packet to prime the heap.
Heap contents (4 items):
  [hit  10.001 (idx=1)]  <-- MIN-HEAP TOP (SMALLEST TIMESTAMP)
  [video 10.002 (idx=0)]
  [ssr   10.003 (idx=2)]
  [adsb  10.150 (idx=3)]

STEP 2: POP, TRANSMIT, AND REPLENISH
- Pop top: [hit 10.001]. Paced and transmitted to ZMQ PUB.
- Read next packet from Reader 1 (returns 10.005) -> Push to Heap.
Heap restructured:
  [video 10.002 (idx=0)]  <-- NEW TOP
  [ssr   10.003 (idx=2)]
  [hit   10.005 (idx=1)]
  [adsb  10.150 (idx=3)]

STEP 3: NEXT ITERATION
- Pop top: [video 10.002]. Paced and transmitted to ZMQ PUB.
- Read next packet from Reader 0 (returns 10.008) -> Push to Heap.
Heap restructured:
  [ssr   10.003 (idx=2)]  <-- NEW TOP
  [hit   10.005 (idx=1)]
  [video 10.008 (idx=0)]
  [adsb  10.150 (idx=3)]
```
Packets across all 4 independent streams are emitted in absolute chronological sequence with $O(\log N)$ efficiency.

---

### 5.3 High-Precision Microsecond Playback Pacing (Hybrid Sleep + Spin-Wait)

How do we achieve true microsecond pacing accuracy in user space?

#### The Operating System Sleep Dilemma:
Standard OS sleep calls (`std::this_thread::sleep_for` or `usleep()`) yield thread execution back to the OS scheduler.
When the OS timer interrupt eventually wakes the thread, **1 to 15 milliseconds have elapsed** (depending on Windows timer resolution or Linux tickless kernel configurations). This coarse granularity is unacceptable for radar replay.

#### Production Solution: Hybrid Sleep + Spin-Wait:
Implemented in [`ReplayEngine.cpp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/replay/ReplayEngine.cpp#L115-L127):

```cpp
// 1. Calculate the target playback timestamp on monotonic steady_clock
uint64_t delta_us = item.msg.timestamp_us - session_start_ts;
auto target_time = real_start + std::chrono::microseconds(delta_us);

// 2. PHASE 1: Coarse OS Sleep
// If target is far away (> 1500 us), sleep to yield CPU cycles to other processes
auto now = std::chrono::steady_clock::now();
if (target_time - now > std::chrono::microseconds(1500)) {
    std::this_thread::sleep_until(target_time - std::chrono::microseconds(1500));
}

// 3. PHASE 2: Fine-Grained Spin-Wait
// Wake up ~1.5 ms early, then spin-wait with hardware CPU pause
while (std::chrono::steady_clock::now() < target_time) {
#if defined(__x86_64__) || defined(_M_X64)
    _mm_pause(); // Intel/AMD hardware PAUSE instruction
#else
    std::this_thread::yield();
#endif
}
```

#### Why the `_mm_pause()` Instruction is Essential:
The intrinsic `_mm_pause()` emits the x86 `PAUSE` instruction:
1. It informs the CPU core pipeline that it is executing a spin-wait loop, preventing memory pipeline order violations and pipeline flushes upon loop exit.
2. It throttles core power consumption, preventing CPU thermal throttling.
3. It enables the pacing loop to achieve **sub-2-microsecond precision** without locking up the operating system.

---

## 6. Implementation Blueprint from Scratch (Checklist & Runbook)

Follow this structured engineering runbook to build the engine from an empty workspace:

### Step 1: Toolchain Setup & `CMakeLists.txt`
1. Set the C++ standard: `set(CMAKE_CXX_STANDARD 20)`.
2. Locate required libraries:
   - `find_package(Qt6 REQUIRED COMPONENTS Core)`
   - `find_library(ZMQ_LIB NAMES zmq ...)`
3. Link targets: `Qt6::Core`, `zmq`, `pthread`.

### Step 2: Core Data Types & Codecs (`src/common/`)
1. Implement [`Types.h`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/common/Types.h):
   - Define constants: `SOP_MARKER = 0x0D0D`, `EOP_MARKER = 0x0A0A`, `HEADER_SIZE = 10`.
   - Define `struct TelemetryMessage` with `topic`, `timestamp_us`, `payload`, and `byteSize()`.
   - Provide `currentUtcMicroseconds()` using `std::chrono::system_clock`.
2. Implement [`EndianCodec.h`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/common/EndianCodec.h):
   - Implement `serializeHeader` and `deserializeHeader` via `qToLittleEndian`/`qToBigEndian`.
   - Implement `verifyFrameMarkers` validating SOP and EOP.
   - Implement `formatSessionTimestamp` (`yyyyMMdd_HHmm`) and `formatSegmentTimestamp` (`yyyyMMdd_HHmmss`).

### Step 3: Recorder Subsystem (`src/recorder/`)
1. Implement [`BoundedByteQueue.h`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/BoundedByteQueue.h):
   - Create thread-safe queue capped at 256 MB.
   - Implement non-blocking `push()` with drop-on-overflow policy.
   - Implement `drain()` via `std::swap` for fast batch transfers.
2. Implement [`TopicWriter.h`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/TopicWriter.h) & [`TopicWriter.cpp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/TopicWriter.cpp):
   - Configure 128 KB buffer via `setvbuf`.
   - Implement Lazy File Creation inside `ensureFileOpen()`.
   - Implement `rollSegment()` for 300-second rollovers.
3. Implement [`RecorderEngine.h`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/RecorderEngine.h) & [`RecorderEngine.cpp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/RecorderEngine.cpp):
   - Run `receiverLoop` on ZMQ SUB socket with 200ms timeout.
   - Run `writerLoop` to dispatch packets to corresponding `TopicWriter` instances.

### Step 4: Replay Subsystem (`src/replay/`)
1. Implement [`TopicReader.h`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/replay/TopicReader.h) & [`TopicReader.cpp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/replay/TopicReader.cpp):
   - Sort and read `.dat` segment files chronologically.
   - Handle seamless transitions across segment boundaries upon EOF.
2. Implement [`ReplayEngine.h`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/replay/ReplayEngine.h) & [`ReplayEngine.cpp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/replay/ReplayEngine.cpp):
   - Bind ZMQ PUB socket and sleep 1000 ms to mitigate Slow-Joiner drops.
   - Prime `std::priority_queue` (Min-Heap) with initial packets from each reader.
   - Execute the playback loop: Pop $\rightarrow$ Hybrid Pacing $\rightarrow$ Multipart Send $\rightarrow$ Replenish Heap.

### Step 5: Command-Line Driver (`src/main.cpp`)
1. Parse CLI options (`--mode=record|replay`, `--sub-endpoint`, `--pub-endpoint`, `--segment-sec`, `--endian`...).
2. Install POSIX signal handlers (`SIGINT`, `SIGTERM`) to trigger graceful shutdown, flushing queued memory buffers to disk before exiting.

### Step 6: Automated Closed-Loop Verification
Once built, validate the entire system end-to-end using [`tests/run_closed_loop_verification.py`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/tests/run_closed_loop_verification.py):
1. Publish 2,000 synthetic test packets across `video`, `hit`, `ssr`, and `adsb`.
2. Record the stream into `session1`.
3. Inspect `.dat` files via [`mcp_zmq_dat_inspector.py`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/tools/mcp_zmq_dat_inspector.py) to verify SOP/EOP markers and timestamp monotonicity.
4. Launch Recorder 2 listening on Replay's port.
5. Replay `session1` into Recorder 2, saving to `session2`.
6. Run byte-by-byte comparison between `session1` and `session2`. The result must be **100% Exact Match with 0 mismatches**.
