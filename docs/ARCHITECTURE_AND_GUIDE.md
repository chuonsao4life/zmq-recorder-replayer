# Hướng Dẫn Kiến Trúc & Cẩm Nang Kỹ Thuật: Hệ Thống Ghi & Phát Lại Dữ Liệu Radar/Cảm Biến Đa Kênh Bằng C++20, ZeroMQ & Qt6

---

## Mục Lục
1. [Tổng quan Bài toán (Big Picture & Luồng dữ liệu)](#1-tổng-quan-bài-toán-big-picture--luồng-dữ-liệu)
   - [1.1 Bối cảnh nghiệp vụ: Radar & Giám sát hàng không / quốc phòng](#11-bối-cảnh-nghiệp-vụ-radar--giám-sát-hàng-không--quốc-phòng)
   - [1.2 Sơ đồ dòng chảy dữ liệu toàn cảnh (End-to-End Pipeline)](#12-sơ-đồ-dòng-chảy-dữ-liệu-toàn-cảnh-end-to-end-pipeline)
   - [1.3 Nhiệm vụ cốt lõi của hai khối Recorder và Replay](#13-nhiệm-vụ-cốt-lõi-của-hai-khối-recorder-và-replay)
2. [Phân tích chi tiết Đầu vào (Input) & Đầu ra (Output)](#2-phân-tích-chi-tiết-đầu-vào-input--đầu-ra-output)
   - [2.1 Giao thức truyền thông mạng: ZeroMQ Multipart Message](#21-giao-thức-truyền-thông-mạng-zeromq-multipart-message)
   - [2.2 Cấu trúc khung nhị phân frame_RDP (Từng Byte Cụ Thể)](#22-cấu-trúc-khung-nhị-phân-frame_rdp-từng-byte-cụ-thể)
   - [2.3 Định dạng lưu trữ trên đĩa: File .dat & Cấu trúc phân mục](#23-định-dạng-lưu-trữ-trên-đĩa-file-dat--cấu-trúc-phân-mục)
   - [2.4 Đi sâu vào kiến trúc máy tính: Căn bệnh Struct Padding](#24-đi-sâu-vào-kiến-trúc-máy-tính-căn-bệnh-struct-padding)
   - [2.5 Cuộc chiến Endianness: Little-Endian vs Big-Endian](#25-cuộc-chiến-endianness-little-endian-vs-big-endian)
3. [Vai trò của từng Tech Stack (Tại sao chọn công cụ này?)](#3-vai-trò-của-từng-tech-stack-tại-sao-chọn-công-cụ-này)
   - [3.1 C++20: Ngôn ngữ lập trình hệ thống hiệu năng cao](#31-c20-ngôn-ngữ-lập-trình-hệ-thống-hiệu-năng-cao)
   - [3.2 ZeroMQ (libzmq / cppzmq): Đột phá vượt xa TCP thuần](#32-zeromq-libzmq--cppzmq-đột-phá-vượt-xa-tcp-thuần)
   - [3.3 Qt6 (QtCore): Chuẩn hóa Endian, Thời gian UTC và Thư mục](#33-qt6-qtcore-chuẩn-hóa-endian-thời-gian-utc-và-thư-mục)
4. [Quy trình xử lý bên trong Recorder (Step-by-Step)](#4-quy-trình-xử-lý-bên-trong-recorder-step-by-step)
   - [4.1 Luồng nhận ReceiverThread: Bắt nhịp thời gian & Kiểm định gói](#41-luồng-nhận-receiverthread-bắt-nhịp-thời-gian--kiểm-định-gói)
   - [4.2 BoundedByteQueue (256 MB): Chống tràn RAM & Hoán đổi theo lô (std::swap)](#42-boundedbytequeue-256-mb-chống-tràn-ram--hoán-đổi-theo-lô-stdswap)
   - [4.3 Luồng ghi WriterThread & Lớp TopicWriter: Buffer 128 KB, Lazy Creation & Cắt File 300s](#43-luồng-ghi-writerthread--lớp-topicwriter-buffer-128-kb-lazy-creation--cắt-file-300s)
5. [Quy trình xử lý bên trong Replay (Step-by-Step)](#5-quy-trình-xử-lý-bên-trong-replay-step-by-step)
   - [5.1 Đọc chuỗi phân đoạn tuần tự với TopicReader](#51-đọc-chuỗi-phân-đoạn-tuần-tự-với-topicreader)
   - [5.2 Thuật toán trộn N kênh theo thứ tự thời gian bằng Min-Heap (N-Way Merge)](#52-thuật-toán-trộn-n-kênh-theo-thứ-tự-thời-gian-bằng-min-heap-n-way-merge)
   - [5.3 Kỹ thuật điều nhịp phát lại chính xác đến Micro giây (Hybrid Pacing)](#53-kỹ-thuật-điều-nhịp-phát-lại-chính-xác-đến-micro-giây-hybrid-pacing)
6. [Cẩm nang xây dựng dự án từ đầu (Implementation Checklist)](#6-cẩm-nang-xây-dựng-dự-án-từ-đầu-implementation-checklist)

---

## 1. Tổng quan Bài toán (Big Picture & Luồng dữ liệu)

### 1.1 Bối cảnh nghiệp vụ: Radar & Giám sát hàng không / quốc phòng
Trong các hệ thống xử lý thông tin radar hiện đại (phòng không, điều hành bay dân dụng ATM/ATC, cảnh giới biển), dữ liệu từ các trạm cảm biến được truyền về trung tâm chỉ huy 24/7 với lưu lượng rất lớn và yêu cầu độ trễ cực thấp.

Hệ thống của chúng ta xử lý 4 luồng dữ liệu chuyên dụng (được phân loại theo `topic`):
1. **`video` (Radar Raw Video / Digitalized Video Sweep)**: Tín hiệu video radar thô đã số hóa từ máy thu sơ cấp. Kênh này có tần suất xung cao nhất, mô tả phản xạ sóng điện từ của địa hình, mây mưa và vỏ kim loại máy bay.
2. **`hit` (Primary Radar Target Hit / Detection Plot)**: Các vết mục tiêu sơ cấp sau khi qua bộ lọc tách ngưỡng thích nghi (CFAR - Constant False Alarm Rate) của đài radar sơ cấp. Mỗi "hit" đại diện cho một phản xạ nghi ngờ là mục tiêu bay tại cự ly và phương vị xác định.
3. **`ssr` (Secondary Surveillance Radar - Radar thứ cấp / IFF)**: Dữ liệu hỏi - đáp chủ động (interrogation/reply) ở tần số 1030/1090 MHz giữa đài mặt đất và máy phát đáp (transponder) trên máy bay. Cung cấp mã nhận dạng Mode 3/A, độ cao áp suất Mode C, hoặc IFF (Nhận diện địch - ta).
4. **`adsb` (Automatic Dependent Surveillance - Broadcast)**: Bản tin giám sát phụ thuộc tự động phát thanh trên tần số 1090 MHz (Extended Squitter), tàu bay định kỳ tự phát tọa độ GPS vệ tinh, vận tốc không khí, góc bay và hô hiệu chuyến bay.

#### Tại sao bài toán "Ghi (Record) & Phát lại (Replay)" lại cực kỳ quan trọng?
- **Tiết kiệm chi phí vận hành**: Mỗi giờ bật một đài radar công suất hàng chục Kilowatt tiêu tốn hàng nghìn số điện và làm hao mòn đèn phát sóng cao tần đắt tiền.
- **Huấn luyện kíp trắc thủ**: Tái hiện lại 100% tình huống thực tế (máy bay tiếp cận dồn dập, thời tiết xấu bão tố, mục tiêu bay thấp né radar) cho học viên thao tác trên màn hình điều khiển.
- **Điều tra sự cố & Tối ưu thuật toán**: Khi xảy ra sự cố suýt va chạm trên không hoặc thuật toán bám bắt (Tracking Filter/Kalman Filter) bị nhảy vết, các kỹ sư cần một bản ghi nhị phân **chính xác đến từng byte** và **chuẩn từng micro giây** để chạy mô phỏng lặp đi lặp lại nhằm sửa lỗi.

---

### 1.2 Sơ đồ dòng chảy dữ liệu toàn cảnh (End-to-End Pipeline)

Hệ thống được thiết kế theo mô hình xử lý phân tán hướng thông điệp (Message-driven Pipeline) với hai phần mềm trung tâm: **Recorder** và **Replay**.

```
 +-------------------------------------------------------------------------------+
 |                        NGUỒN DỮ LIỆU THỰC TẾ HOẶC GIẢ LẬP                     |
 |  [appmrepl] Đọc file dữ liệu gốc (.rdata) -> Phát luồng TCP thô Port 8889     |
 +-------------------------------------------------------------------------------+
                                         |
                                         | Luồng TCP thuần (Byte Stream)
                                         v
 +-------------------------------------------------------------------------------+
 |                              (1) ZMQ BRIDGE                                   |
 | - Kết nối TCP tới 127.0.0.1:8889 (Tự động reconnect khi đứt kết nối).         |
 | - Phân tách luồng byte thành các khung [frame_RDP] hoàn chỉnh.                |
 | - Đọc byte TYPE -> Ánh xạ thành nhãn topic: "video", "hit", "ssr", "adsb".    |
 | - Gửi thông điệp ZeroMQ Multipart: [Part 1: Topic][Part 2: frame_RDP].        |
 | - BIND Socket ZMQ PUB: tcp://192.168.5.21:30003                               |
 +-------------------------------------------------------------------------------+
                                         |
                                         | Mạng LAN nội bộ
                                         | ZeroMQ PUB / SUB Message Stream
                                         v
 +-------------------------------------------------------------------------------+
 |                              (2) RECORDER ENGINE                              |
 | - Socket ZMQ SUB kết nối tới tcp://192.168.5.21:30003                         |
 | - Đăng ký nhận 4 topics: "video", "hit", "ssr", "adsb".                       |
 | - Đóng gói mỗi bản tin: [Timestamp u64 (8B)][Length u16 (2B)][frame_RDP]       |
 | - Hàng đợi trung chuyển không block (BoundedByteQueue <= 256 MB).             |
 | - Cắt file phân đoạn định kỳ mỗi 300 giây (Segment Rollover).                 |
 | - Cơ chế Lazy Creation: Chỉ tạo file khi topic có dữ liệu thực tế.            |
 +-------------------------------------------------------------------------------+
                                         |
                                         | Ghi trực tiếp xuống ổ cứng NVMe / SSD
                                         v
 +-------------------------------------------------------------------------------+
 |                      HỆ THỐNG FILE LƯU TRỮ TRÊN Ổ CỨNG                        |
 |                                                                               |
 |  <out_dir>/                                                                   |
 |  └── 20260925_0926/                  <-- Thư mục Session (Thời điểm bắt đầu) |
 |      └── zdata/                                                               |
 |          ├── video/video_20260925_092600.dat  <-- File phân đoạn 300 giây     |
 |          ├── hit/hit_20260925_092600.dat                                      |
 |          ├── ssr/ssr_20260925_092600.dat                                      |
 |          └── adsb/adsb_20260925_092600.dat                                    |
 +-------------------------------------------------------------------------------+
                                         |
                                         | Đọc tuần tự các file .dat từng topic
                                         v
 +-------------------------------------------------------------------------------+
 |                               (3) REPLAY ENGINE                               |
 | - Khởi tạo TopicReader cho từng thư mục topic.                                |
 | - Dùng cấu trúc Min-Heap (std::priority_queue) trộn 4 luồng theo time_stamp.  |
 | - BIND Socket ZMQ PUB: tcp://192.168.5.31:30004                               |
 | - Khắc phục "Slow Joiner": Tạm dừng 1000 ms chờ Subscriber kết nối.           |
 | - Điều nhịp vi mô (Hybrid Sleep + Spin-wait _mm_pause) chuẩn từng micro giây. |
 | - Bắn gói tin [topic][frame_RDP] ra mạng y hệt luồng trực tiếp ban đầu.       |
 +-------------------------------------------------------------------------------+
                                         |
                                         | tcp://192.168.5.31:30004
                                         v
 +-------------------------------------------------------------------------------+
 |                        CÁC HỆ THỐNG TIÊU THỤ (SUBSCRIBERS)                    |
 | - Màn hình hiển thị radar chiến thuật (Radar Tactical PPI Display).           |
 | - Bộ tích hợp xử lý thông tin radar (Multi-Radar Data Fusion System).         |
 | - Bộ ghi âm/ghi hình buồng lái và phòng chỉ huy tác chiến C2.                 |
 +-------------------------------------------------------------------------------+
```

---

### 1.3 Nhiệm vụ cốt lõi của hai khối Recorder và Replay
- **Recorder (Khối Ghi)**: Phải hoạt động liên tục với hiệu suất cực cao. Trong điều kiện dòng dữ liệu dồn về với tốc độ hàng chục nghìn gói mỗi giây, Recorder không được phép giật lag, không làm rơi rớt gói tin trên socket mạng, không rò rỉ bộ nhớ, và đảm bảo file lưu trữ có cấu trúc nhị phân chuẩn xác.
- **Replay (Khối Phát lại)**: Phải đảm bảo tính trung thực tuyệt đối (Fidelity). Khoảng cách thời gian giữa gói tin số 1 và gói tin số 2 phát ra phải chuẩn xác tương đối so với thời điểm chúng được ghi nhận ngoài thực địa với độ trễ vi mô (microsecond precision). Đồng thời, gói tin phát ra và gói tin ghi lại phải trùng khớp **100% từng byte** (Byte-Exact Verification).

---

## 2. Phân tích chi tiết Đầu vào (Input) & Đầu ra (Output)

Đối với một lập trình viên C++, việc làm chủ bản chất của từng byte dữ liệu di chuyển trên dây cáp mạng và lưu xuống phiến đĩa là điều kiện tiên quyết.

### 2.1 Giao thức truyền thông mạng: ZeroMQ Multipart Message
Trên đường truyền mạng giữa ZMQ Bridge -> Recorder (cổng `30003`) và Replay -> Subscribers (cổng `30004`), dữ liệu không đi theo một chuỗi byte dính liền như TCP thông thường, mà được ZeroMQ đóng gói dưới dạng **Bản tin đa phần (Multipart Message)** gồm đúng 2 phần:

```
+-------------------------------------------------------------------+
|                   ZEROMQ MULTIPART MESSAGE                        |
|                                                                   |
|   +--------------------------+   +----------------------------+   |
|   |  Part 1: Topic (ASCII)   |   |   Part 2: frame_RDP (Raw)  |   |
|   |  "video", "hit", ...     |   |   Dữ liệu nhị phân RDP     |   |
|   +--------------------------+   +----------------------------+   |
|      (Cờ ZMQ_SNDMORE = true)        (Cờ ZMQ_SNDMORE = false)      |
+-------------------------------------------------------------------+
```

- **Part 1 (Topic String)**: Chuỗi ký tự ASCII biểu diễn tên chủ đề (ví dụ: `"video"`, `"hit"`, `"ssr"`, `"adsb"`). Nhãn này giúp ZeroMQ lọc gói tin ngay tại tầng mạng (xem mục 3.2).
- **Part 2 (frame_RDP)**: Toàn bộ mảng byte nhị phân của khung tin RDP, được giữ nguyên vẹn 100%, không bị sửa đổi bất kỳ byte nào.

---

### 2.2 Cấu trúc khung nhị phân frame_RDP (Từng Byte Cụ Thể)
Khung tin RDP (Radar Data Protocol) là tiêu chuẩn nhị phân công nghiệp dùng để đóng gói dữ liệu radar. Mỗi frame có kích thước tối thiểu là **12 bytes** (khi trường `DATA` có kích thước 0 byte).

Dưới đây là sơ đồ bố trí bộ nhớ chi tiết của một `frame_RDP`:

```
Byte Offset:
 0       1       2       3       4       5       6       7       8 ... 8+LEN-1  8+LEN   9+LEN   10+LEN  11+LEN
+-------+-------+-------+-------+-------+-------+-------+-------+-------------+-------+-------+-------+-------+
|  SOP (0x0D0D) |  SEQ (u16)    |SRC_ID | TYPE  |  LEN (u16)    | DATA (bytes)| CHKSUM (u16)  |  EOP (0x0A0A) |
+-------+-------+-------+-------+-------+-------+-------+-------+-------------+-------+-------+-------+-------+
|<--- 2 Bytes ->|<--- 2 Bytes ->|<-1 B->|<-1 B->|<--- 2 Bytes ->|<- LEN Bytes>|<--- 2 Bytes ->|<--- 2 Bytes ->|
```

#### Bảng định nghĩa chi tiết từng trường:
| Tên trường | Kiểu dữ liệu | Kích thước | Giá trị mẫu / Quy ước | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- | :--- | :--- |
| **SOP** | `uint16_t` | 2 Bytes | `0x0D0D` | **Start of Packet**: Dấu hiệu nhận biết điểm bắt đầu của một khung tin. |
| **SEQ** | `uint16_t` | 2 Bytes | `0x0001`, `0x0002`... | **Sequence Number**: Số thứ tự tăng dần đều để phát hiện mất gói tin. |
| **SRC_ID**| `uint8_t` | 1 Byte | `0x21` | **Source ID**: Mã định danh trạm radar phát tín hiệu. |
| **TYPE** | `uint8_t` | 1 Byte | `0x01, 0x02, 0x10...` | **Message Type**: Loại dữ liệu, quyết định ánh xạ sang topic nào. |
| **LEN** | `uint16_t` | 2 Bytes | `0x0080` (128 bytes) | **Payload Length**: Kích thước chiều dài của phần trường `DATA`. |
| **DATA** | `uint8_t[]` | `LEN` Bytes | Mảng byte nhị phân | Dữ liệu cảm biến chuyên biệt (tọa độ mục tiêu, vệt quét...). |
| **CHKSUM**| `uint16_t` | 2 Bytes | Tổng bù 16-bit | Tổng kiểm tra lỗi toàn vẹn khung tin. |
| **EOP** | `uint16_t` | 2 Bytes | `0x0A0A` | **End of Packet**: Dấu hiệu kết thúc khung tin RDP. |

#### Bảng ánh xạ từ mã `TYPE` sang `topic`:
Quy tắc chuyển đổi được quy định rõ trong tài liệu kỹ thuật:
- `TYPE = 0x01` $\rightarrow$ Ánh xạ sang topic **`video`**
- `TYPE = 0x02` $\rightarrow$ Ánh xạ sang topic **`hit`**
- `TYPE = 0x10` hoặc `0x11` $\rightarrow$ Ánh xạ sang topic **`ssr`**
- `TYPE = 0xF1` $\rightarrow$ Ánh xạ sang topic **`adsb`**

Trong mã nguồn [`EndianCodec.h`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/common/EndianCodec.h#L33-L45), hàm `verifyFrameMarkers` kiểm tra tính hợp lệ của hai đầu mút `SOP` và `EOP`:
```cpp
inline bool verifyFrameMarkers(const uint8_t* data, size_t len, bool useBigEndian) {
    if (len < 12) return false;  // Tối thiểu phải đủ 12 bytes
    uint16_t sop = useBigEndian ? qFromBigEndian<uint16_t>(data) : qFromLittleEndian<uint16_t>(data);
    uint16_t eop = useBigEndian ? qFromBigEndian<uint16_t>(data + len - 2) : qFromLittleEndian<uint16_t>(data + len - 2);
    return (sop == SOP_MARKER) && (eop == EOP_MARKER);
}
```

---

### 2.3 Định dạng lưu trữ trên đĩa: File `.dat` & Cấu trúc phân mục

#### Cấu trúc cây thư mục:
Mọi dữ liệu ghi lại được tổ chức theo cấu trúc phân cấp chặt chẽ:
```
<out_dir>/
└── yyyyMMdd_HHmm/                     <-- Thời điểm khởi động phiên ghi (UTC Session)
    └── zdata/
        ├── video/
        │   ├── video_20260925_092600.dat  <-- Phân đoạn 300 giây thứ 1 (09:26:00 -> 09:31:00)
        │   └── video_20260925_093100.dat  <-- Phân đoạn 300 giây thứ 2 (09:31:00 -> 09:36:00)
        ├── hit/
        │   └── hit_20260925_092600.dat
        ├── ssr/
        │   └── ssr_20260925_092600.dat
        └── adsb/
            └── adsb_20260925_092600.dat
```
> [!IMPORTANT]
> **Quy ước định dạng mốc thời gian**:
> - Tên thư mục Session cấp 1 dùng định dạng: `yyyyMMdd_HHmm` (ví dụ: `20260925_0926` - chính xác đến phút theo chuẩn giờ UTC). Được tạo bởi hàm [`formatSessionTimestamp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/common/EndianCodec.h#L47-L51).
> - Tên file `.dat` cấp con dùng định dạng: `<topic>_yyyyMMdd_HHmmss.dat` (ví dụ: `video_20260925_092600.dat` - chính xác đến giây theo chuẩn giờ UTC, đại diện cho mốc đầu của cửa sổ 300 giây). Được tạo bởi hàm [`formatSegmentTimestamp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/common/EndianCodec.h#L53-L57).

#### Bố cục gói tin trong File `.dat` (The 10-Byte Header Protocol):
Trong mỗi file `.dat`, các bản tin được ghi nối tiếp nhau liên tục (**Back-to-back, không chèn byte rác, không padding**). Tên topic **không được lưu** vào trong file vì bản thân đường dẫn thư mục và tên file đã thể hiện tên topic.

Mỗi bản tin lưu trữ gồm đúng 2 phần: **Header 10 bytes** và **Payload RDP**:

```
+----------------------------------------------------------------------------------------+
|                          MỘT BẢN TIN TRONG FILE .DAT                                   |
|                                                                                        |
|  0               7 8         9 10                                            10+data_len|
| +-----------------+-----------+--------------------------------------------------------+|
| |  time_stamp u64 | data_len  |                      frame_RDP                         ||
| |  (8 Bytes)      | (2 Bytes) |                  (data_len Bytes)                      ||
| +-----------------+-----------+--------------------------------------------------------+|
| |<-- 10-Byte Fixed Header --->|<---------------- Variable Payload -------------------->||
+----------------------------------------------------------------------------------------+
```

- **`time_stamp` (8 Bytes - `uint64_t`)**: Số micro giây (microseconds) tính từ mốc Unix Epoch (`1970-01-01 00:00:00 UTC`) tại thời điểm Recorder nhận được thông điệp từ mạng.
- **`data_len` (2 Bytes - `uint16_t`)**: Chiều dài chính xác của `frame_RDP` đi kèm ngay phía sau.
- **`frame_RDP` (`data_len` Bytes)**: Toàn bộ khung tin RDP (bắt đầu từ `SOP 0x0D0D` đến `EOP 0x0A0A`).

Tổng dung lượng chiếm dụng của một bản tin trên đĩa = `10 + data_len` bytes.

---

### 2.4 Đi sâu vào kiến trúc máy tính: Căn bệnh Struct Padding

Rất nhiều lập trình viên C++ mới vào nghề thường có thói quen viết mã lưu file như sau:
```cpp
// !!! NGUY HIỂM - ĐOẠN CODE SAI LẦM KINH ĐIỂN !!!
struct FileHeader {
    uint64_t time_stamp; // 8 bytes
    uint16_t data_len;   // 2 bytes
}; // Bạn nghĩ rằng sizeof(FileHeader) == 10 bytes?

FileHeader hdr{ts, len};
fwrite(&hdr, sizeof(hdr), 1, fp); // THẢM HỌA XẢY RA Ở ĐÂY!
```

#### Tại sao lại là thảm họa?
Các bộ vi xử lý hiện đại (x86_64, ARM64) truy xuất bộ nhớ hiệu quả nhất khi dữ liệu được căn lề theo lũy số của kích thước từ lệnh (Word boundary), tức là địa chỉ chia hết cho 8 bytes đối với số 64-bit.
Khi bạn khai báo một `struct` gồm một số 8 bytes (`uint64_t`) và một số 2 bytes (`uint16_t`):
- Trình biên dịch (GCC, Clang, MSVC) sẽ tự động chèn thêm **6 bytes đệm vô hình (Struct Padding)** vào sau biến `uint16_t` để toàn bộ `struct` có kích thước là bội số của 8:
  $$8 \text{ bytes (timestamp)} + 2 \text{ bytes (data\_len)} + \mathbf{6 \text{ bytes (padding)}} = \mathbf{16 \text{ bytes}}!$$

```
Bố trí trong RAM khi dùng C++ Struct mặc định:
[Byte 0 .. Byte 7] : time_stamp (8B)
[Byte 8 .. Byte 9] : data_len   (2B)
[Byte 10.. Byte 15]: PADDING RÁC (6B) <--- Compiler tự chèn vào!
```

Nếu bạn dùng `fwrite(&hdr, sizeof(hdr), 1, fp)`, bạn vừa vô tình ghi 6 byte rác vào file. Khi phần mềm Replay đọc 10 byte tiếp theo, nó sẽ đọc nhầm 6 byte rác này vào phần đầu của `frame_RDP`, dẫn tới lệch toàn bộ offset, mất dấu `SOP`, và toàn bộ file dữ liệu bị hỏng hoàn toàn!

#### Giải pháp chuẩn công nghiệp trong dự án:
Dự án **tuyệt đối không ghi trực tiếp struct ra đĩa**. Thay vào đó, trong [`TopicWriter.cpp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/TopicWriter.cpp#L60-L65) và [`EndianCodec.h`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/common/EndianCodec.h#L13-L21), chúng ta cấp phát một mảng byte thuần túy `uint8_t hdr[10]` và tuần tự hóa (serialize) từng trường một cách tường minh:
```cpp
inline void serializeHeader(uint64_t timestamp_us, uint16_t data_len, uint8_t* dest, bool useBigEndian) {
    if (useBigEndian) {
        qToBigEndian(timestamp_us, dest);       // Ghi đúng 8 byte vào dest[0..7]
        qToBigEndian(data_len, dest + 8);       // Ghi đúng 2 byte vào dest[8..9]
    } else {
        qToLittleEndian(timestamp_us, dest);    // Ghi đúng 8 byte vào dest[0..7]
        qToLittleEndian(data_len, dest + 8);    // Ghi đúng 2 byte vào dest[8..9]
    }
}
```
Nhờ kỹ thuật này, kích thước header lưu trữ luôn luôn cố định chính xác **10.0000% bytes**, không có bất kỳ một byte padding nào tồn tại!

---

### 2.5 Cuộc chiến Endianness: Little-Endian vs Big-Endian

Một khái niệm sống còn trong kỹ thuật mạng và lưu trữ nhị phân là **Thứ tự sắp xếp byte (Endianness)**:
- **Little-Endian (LE)**: Byte có trọng số thấp nhất (LSB - Least Significant Byte) được lưu ở địa chỉ ô nhớ thấp nhất. Đây là chuẩn mặc định của hầu hết CPU máy tính thương mại hiện nay (Intel, AMD x86/x64, Apple Silicon ARM).
- **Big-Endian (BE)**: Byte có trọng số cao nhất (MSB - Most Significant Byte) được lưu ở địa chỉ ô nhớ thấp nhất. Đây được gọi là **Network Byte Order** và là chuẩn của nhiều máy tính quân sự, viễn thông cũ (PowerPC, SPARC, máy tính chuyên dụng trên khoang radar).

#### Ví dụ trực quan so sánh 2 cách ghi nhớ:
Giả sử ta cần ghi giá trị `time_stamp = 0x0102030405060708` (8 bytes) và `data_len = 0x0080` (128 bytes - 2 bytes) vào mảng 10 bytes:

```
Địa chỉ Offset:     [0]   [1]   [2]   [3]   [4]   [5]   [6]   [7]   [8]   [9]
--------------------------------------------------------------------------------
Little-Endian:      0x08  0x07  0x06  0x05  0x04  0x03  0x02  0x01  0x80  0x00
(Đảo ngược byte)   |<------- time_stamp (LSB trước) ------->|  |<data_len>|

Big-Endian:         0x01  0x02  0x03  0x04  0x05  0x06  0x07  0x08  0x00  0x80
(Đúng thứ tự đọc)  |<------- time_stamp (MSB trước) ------->|  |<data_len>|
```

Hệ thống của chúng ta hỗ trợ cả hai thông qua tham số dòng lệnh `--endian=little` hoặc `--endian=big`. Nhờ sử dụng template của Qt `<QtEndian>` (`qToLittleEndian`, `qToBigEndian`), việc hoán đổi byte được vi điều khiển thực hiện tức thì bằng các tập lệnh phần cứng asm (lệnh `bswap` trên x86), không tiêu tốn chu kỳ tính toán của CPU.

---

## 3. Vai trò của từng Tech Stack (Tại sao chọn công cụ này?)

Mỗi công nghệ trong hệ thống được lựa chọn kỹ lưỡng để giải quyết một bài toán vật lý cụ thể, không thừa và không thiếu.

### 3.1 C++20: Ngôn ngữ lập trình hệ thống hiệu năng cao

| Thành phần C++20 | File sử dụng trong dự án | Mục đích kỹ thuật |
| :--- | :--- | :--- |
| **`std::thread` & `std::mutex`** | [`RecorderEngine.cpp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/RecorderEngine.cpp#L43-L44) | Tách biệt hoàn toàn luồng thu nhận mạng (Receiver) và luồng ghi ổ cứng (Writer), ngăn hiện tượng I/O đĩa làm nghẽn socket mạng. |
| **`std::condition_variable`** | [`BoundedByteQueue.h`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/BoundedByteQueue.h#L51-L54) | Đưa luồng Writer vào trạng thái "ngủ sâu" (Sleep) khi không có gói tin, và "đánh thức" tức thì khi có dữ liệu đến mà không ngốn 100% CPU. |
| **`std::atomic` (Memory Ordering)** | [`BoundedByteQueue.h`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/BoundedByteQueue.h#L66) | Điều khiển biến cờ dừng `stop_requested_` bằng `memory_order_release` và `memory_order_acquire`. Giúp đồng bộ an toàn giữa các lõi CPU mà không cần dùng Mutex nặng nề. |
| **Zero-Copy & `std::move`** | [`BoundedByteQueue.h`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/BoundedByteQueue.h#L38) | Chuyển giao quyền sở hữu bộ nhớ của mảng byte vector từ luồng nhận sang hàng đợi và luồng ghi. Tránh hoàn toàn việc `memcpy` hàng chục Megabyte dữ liệu trong bộ nhớ RAM. |
| **`std::priority_queue` (Min-Heap)**| [`ReplayEngine.cpp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/replay/ReplayEngine.cpp#L73) | Sắp xếp và trộn $N$ luồng dữ liệu độc lập theo thứ tự thời gian với độ phức tạp thuật toán cực nhỏ: $O(\log N)$. |
| **`std::chrono::system_clock`** | [`Types.h`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/common/Types.h#L33-L36) | Lấy thời gian thực Epoch UTC để gán nhãn thời gian cho dữ liệu lưu trữ. |
| **`std::chrono::steady_clock`** | [`ReplayEngine.cpp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/replay/ReplayEngine.cpp#L90) | Đồng hồ đơn điệu (Monotonic clock). Đảm bảo thời gian trôi luôn tiến về phía trước, không bị giật lùi nếu hệ điều hành cập nhật giờ NTP, cốt lõi cho thuật toán điều nhịp Replay. |

---

### 3.2 ZeroMQ (`libzmq` / `cppzmq`): Đột phá vượt xa TCP thuần

Một trong những câu hỏi phổ biến nhất của các lập trình viên mới là: **"Tại sao không dùng trực tiếp TCP Socket thuần (Berkeley Socket) của hệ điều hành mà phải dùng ZeroMQ?"**

Hãy xem bảng so sánh bản chất dưới đây:

```
+------------------------------------+------------------------------------+
|         TCP SOCKET THUẦN           |             ZEROMQ                 |
+------------------------------------+------------------------------------+
| 1. Luồng Byte (Byte-Stream):       | 1. Luồng Thông điệp (Message):     |
| TCP không biết khái niệm "gói tin".| ZMQ bảo toàn tuyệt đối ranh giới   |
| Dữ liệu gửi đi có thể bị dính chùm | của từng gói tin (Atomic Frames).  |
| (Packet Coalescing/Nagle) hoặc bị  | Nhận đúng kích thước đã gửi, không |
| xé vụn (Fragmentation). Lập trình  | cần tự viết code ghép byte rườm rà.|
| viên phải tự viết mã dò SOP/EOP.   |                                    |
+------------------------------------+------------------------------------+
| 2. Kết nối cứng nhắc 1-1:          | 2. Mô hình PUB/SUB linh hoạt:      |
| Phải tự quản lý Accept, Listen,    | Một bên phát (PUB), hàng chục bên  |
| Connect, Reconnect khi rớt mạng.   | nhận (SUB). Tự động kết nối lại khi|
|                                    | đường truyền mạng bị đứt.          |
+------------------------------------+------------------------------------+
| 3. Lọc dữ liệu thủ công:           | 3. Lọc Topic tự động tại nguồn:    |
| Phải nhận toàn bộ dữ liệu về ứng   | Socket SUB lọc topic ngay tại tầng |
| dụng rồi mới kiểm tra có cần không.| mạng. Gói tin không thuộc topic đã |
| Gây lãng phí băng thông và CPU.    | đăng ký sẽ bị loại bỏ lập tức.     |
+------------------------------------+------------------------------------+
```

#### Hai tính năng đặc thù của ZMQ được khai thác tối đa trong dự án:

1. **Cơ chế lọc Topic tự động ở `ZMQ_SUB`**:
   Trong [`RecorderEngine.cpp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/RecorderEngine.cpp#L75-L77), Recorder chỉ cần gọi:
   ```cpp
   for (const auto& topic : config_.topics) {
       sub_sock.set(zmq::sockopt::subscribe, topic);
   }
   ```
   Nếu ZMQ Bridge bắn ra một topic lạ (ví dụ `"weather"`), tầng C-core của ZeroMQ sẽ loại bỏ nó ngay lập tức trước khi cấp phát bộ nhớ đưa lên tầng ứng dụng C++.

2. **Căn bệnh "Slow Joiner" và giải pháp 1000ms**:
   Trong mô hình ZeroMQ PUB/SUB qua giao thức `tcp://`, socket PUB không hề biết khi nào thì các Subscriber đã hoàn tất bắt tay TCP (TCP 3-way handshake) và gửi xong bộ lọc đăng ký (subscription filter).
   Nếu bạn vừa gọi `pub_sock.bind()` mà đã bắn dữ liệu ngay ở dòng lệnh tiếp theo, **vài trăm đến vài nghìn gói tin đầu tiên sẽ bị rơi vào "hố đen"** vì Subscriber chưa kịp kết nối xong!
   Để giải quyết triệt để vấn đề này, [`ReplayEngine.cpp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/replay/ReplayEngine.cpp#L68-L70) áp dụng nguyên tắc:
   ```cpp
   std::cout << "[ReplayEngine] Waiting " << config_.slow_joiner_ms
             << " ms for subscribers to connect (slow-joiner mitigation)..." << std::endl;
   std::this_thread::sleep_for(std::chrono::milliseconds(config_.slow_joiner_ms));
   ```
   Khoảng nghỉ 1000ms này đảm bảo 100% các Subscriber trong mạng nội bộ đã sẵn sàng trước khi gói tin đầu tiên được phát ra.

---

### 3.3 Qt6 (`QtCore`): Chuẩn hóa Endian, Thời gian UTC và Thư mục
Thay vì phụ thuộc vào các hàm C POSIX hay Win32 API cồng kềnh, phân tán:
- **`<QtEndian>`**: Cung cấp các hàm `qToLittleEndian`, `qToBigEndian`, `qFromLittleEndian`, `qFromBigEndian`. Đây là các template hàm an toàn tuyệt đối về kiểu dữ liệu (Type-safe), tự động tương thích từ số nguyên 16-bit đến 64-bit.
- **`QDateTime` & `QTimeZone::UTC`**: Đảm bảo toàn bộ mốc thời gian chuyển đổi sang chuỗi `yyyyMMdd_HHmm` đều chạy theo chuẩn giờ quốc tế UTC, không bị ảnh hưởng bởi múi giờ địa phương của máy tính hay quy ước giờ mùa hè (Daylight Saving Time - DST).
- **`QDir::mkpath`**: Tự động tạo thư mục đa tầng (tương đương `mkdir -p` trên Linux) một cách an toàn trên cả Windows và Linux mà không cần quan tâm dấu gạch chéo `/` hay gạch chéo ngược `\`.

---

## 4. Quy trình xử lý bên trong Recorder (Step-by-Step)

Mô-đun Recorder được thiết kế theo kiến trúc **Producer - Consumer** phân tách bằng hàng đợi giới hạn dung lượng:

```
                    KIẾN TRÚC BÊN TRONG KHỐI RECORDER
                    
 [ZMQ SUB Socket] (tcp://192.168.5.21:30003)
        |
        v
 +-------------------------------------------------------+
 |               LUỒNG 1: RECEIVER THREAD                |
 | 1. sub_sock.recv(topic) & recv(payload)               |
 | 2. Gắn nhãn thời gian: currentUtcMicroseconds()       |
 | 3. Kiểm tra tính hợp lệ: verifyFrameMarkers()         |
 | 4. Đóng gói vào struct TelemetryMessage               |
 +-------------------------------------------------------+
        |
        | push(std::move(msg))
        v
 +-------------------------------------------------------+
 |            HÀNG ĐỢI: BOUNDED BYTE QUEUE               |
 | - Giới hạn bộ nhớ: max_bytes = 256 MB                 |
 | - Chính sách: Drop-on-Overflow (Bỏ gói mới nếu đầy)   |
 | - Đồng bộ: std::mutex + std::condition_variable       |
 +-------------------------------------------------------+
        |
        | drain(batch) thông qua std::swap (Cực nhanh)
        v
 +-------------------------------------------------------+
 |                LUỒNG 2: WRITER THREAD                 |
 | 1. Duyệt từng message trong batch đã rút ra.          |
 | 2. Tính chỉ số phân đoạn: seg_idx = delta / 300s.     |
 | 3. Nếu chuyển phân đoạn -> Gọi rollSegment().         |
 | 4. Tìm hoặc tạo TopicWriter tương ứng.                |
 | 5. Ghi dữ liệu thông qua buffer đệm 128 KB.          |
 +-------------------------------------------------------+
        |
        +----------> [video_*.dat] (Buffer 128KB)
        +----------> [hit_*.dat]   (Buffer 128KB)
        +----------> [ssr_*.dat]   (Buffer 128KB)
        +----------> [adsb_*.dat]  (Buffer 128KB)
```

### 4.1 Luồng nhận ReceiverThread: Bắt nhịp thời gian & Kiểm định gói
Được cài đặt trong hàm [`RecorderEngine::receiverLoop`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/RecorderEngine.cpp#L67-L138):

1. **Khởi tạo kết nối & Cấu hình Timeout**:
   - Thiết lập tùy chọn `rcvtimeo = 200` ms. Cứ sau mỗi 200ms nếu không có gói tin nào đến, lệnh `sub_sock.recv()` sẽ trả về `false`. Điều này giúp luồng nhận thoát khỏi trạng thái phong tỏa (Blocking) để kiểm tra xem hệ thống có yêu cầu dừng (`stop_requested_`) hay không, phục vụ cơ chế tắt nhẹ nhàng (Graceful Shutdown khi ấn `Ctrl+C`).
2. **Bắt nhãn thời gian UTC micro giây**:
   - Ngay khoảnh khắc hàm `recv()` trả về phần payload, lệnh `currentUtcMicroseconds()` được gọi ngay lập tức. Đây là thời điểm chính xác nhất mà gói tin chạm tới tầng ứng dụng, loại bỏ độ trễ xếp hàng sau này.
3. **Kiểm tra tính toàn vẹn (Integrity Verification)**:
   - Gọi `verifyFrameMarkers(payload_ptr, payload_len, config_.use_big_endian)`.
   - Nếu gói tin bị cụt (`len < 12`), hoặc đầu `SOP` không phải `0x0D0D`, hoặc đuôi `EOP` không phải `0x0A0A`, gói tin sẽ bị loại bỏ ngay lập tức và tăng biến đếm `total_invalid_`.
4. **Chuyển quyền sở hữu vào Hàng đợi**:
   - Dữ liệu được đưa vào struct [`TelemetryMessage`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/common/Types.h#L15-L24) và gọi `queue_->push(std::move(msg))`. Không có thao tác nhân bản vùng nhớ (Zero-copy).

---

### 4.2 BoundedByteQueue (256 MB): Chống tràn RAM & Hoán đổi theo lô (`std::swap`)
Được cài đặt trong [`BoundedByteQueue.h`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/BoundedByteQueue.h). Đây là thành phần quyết định sự ổn định của hệ thống khi chạy tải cao.

#### 1. Chính sách chống tràn RAM: Drop-on-Overflow
Trong các hệ thống thực tế, nếu ổ đĩa bị nghẽn (ví dụ do Windows Update hoặc ổ đĩa quá nhiệt giảm tốc độ ghi), dữ liệu dồn về sẽ làm RAM tăng phi mã dẫn tới sập ứng dụng (Out of Memory - OOM Crash).
- `BoundedByteQueue` quản lý dung lượng theo **tổng số byte thực tế** (`current_bytes_`), không đếm số lượng gói tin ảo. Dung lượng tính toán gồm cả overhead của struct và dung lượng buffer nội tại của `std::string` và `std::vector` (xem `TelemetryMessage::byteSize()`).
- Khi thêm một gói tin làm tổng kích thước vượt quá ngưỡng 256 MB (`max_bytes_`):
  ```cpp
  if (current_bytes_ + msg_size > max_bytes_) {
      dropped_messages_.fetch_add(1, std::memory_order_relaxed);
      dropped_bytes_.fetch_add(msg_size, std::memory_order_relaxed);
      return false; // DROP NGAY LẬP TỨC!
  }
  ```
- **Nguyên tắc vàng**: Luồng nhận **KHÔNG BAO GIỜ ĐƯỢC CHỜ (Never Block Receiver)**. Thà làm rơi gói tin mới nhất và ghi nhận thống kê còn hơn để luồng nhận bị treo, dẫn tới tràn hàng đợi mạng của hệ điều hành.

#### 2. Kỹ thuật rút dữ liệu siêu nhanh: Batch Swapping (`std::swap`)
Thông thường, nếu luồng ghi cứ mỗi lần lấy 1 gói tin lại khóa `std::mutex`, hiện tượng **Tranh chấp khóa (Lock Contention)** sẽ làm luồng nhận bị khựng lại liên tục.
Dự án sử dụng kỹ thuật hoán đổi con trỏ hai hàng đợi [`std::swap`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/BoundedByteQueue.h#L57):
```cpp
bool drain(std::deque<TelemetryMessage>& dest, std::chrono::milliseconds timeout) {
    dest.clear();
    std::unique_lock<std::mutex> lock(mutex_);
    // Chờ đến khi có dữ liệu hoặc có yêu cầu dừng
    if (queue_.empty() && !stop_requested_) {
        cv_.wait_for(lock, timeout, [this]() { return !queue_.empty() || stop_requested_; });
    }

    if (!queue_.empty()) {
        std::swap(queue_, dest); // HOÁN ĐỔI TRONG VÀI NANO GIÂY!
        current_bytes_ = 0;
        return true;
    }
    return false;
}
```
Sau khi `std::swap` thực hiện xong, toàn bộ lô gói tin (có thể gồm hàng nghìn gói) đã nằm gọn trong biến `dest` của Luồng Ghi. Khóa Mutex được giải phóng ngay lập tức. Luồng Ghi thong thả ghi hàng nghìn gói ra đĩa mà không hề làm phiền Luồng Nhận!

---

### 4.3 Luồng ghi WriterThread & Lớp `TopicWriter`: Buffer 128 KB, Lazy Creation & Cắt File 300s

Cài đặt trong [`RecorderEngine::writerLoop`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/RecorderEngine.cpp#L140-L183) và [`TopicWriter.cpp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/TopicWriter.cpp):

#### 1. Bộ đệm người dùng 128 KB (`setvbuf`):
Mỗi lần gọi `fwrite()` trực tiếp ra đĩa là một lần tiêu tốn lời gọi hệ thống (System Call `sys_write`), làm chậm hiệu năng I/O.
Trong `TopicWriter`, chúng ta cấp phát riêng một bộ đệm 128 KB cho mỗi file:
```cpp
constexpr size_t IO_BUFFER_SIZE = 128 * 1024; // 128 KB
std::vector<char> io_buffer_(IO_BUFFER_SIZE);
std::setvbuf(file_, io_buffer_.data(), _IOFBF, io_buffer_.size());
```
Cờ `_IOFBF` (Full Buffering) chỉ thị cho thư viện C chuẩn chỉ ghi xuống đĩa cứng khi bộ đệm 128 KB đã đầy, tối ưu hóa triệt để tốc độ ghi cho các dòng ổ cứng SSD NVMe.

#### 2. Cơ chế Lazy File Creation (Tránh tạo file rỗng):
Trong thực tế, một đài radar phụ (ví dụ SSR hoặc ADSB) có thể bị tắt bảo dưỡng suốt nhiều giờ. Nếu hệ thống cứ định kỳ 300s lại tạo sẵn file cho tất cả các topic, sau một ngày ổ đĩa sẽ đầy rác với hàng nghìn file 0-byte rỗng.
- Khi khởi động phân đoạn mới, `TopicWriter::rollSegment()` chỉ đơn giản là đóng file cũ và gán con trỏ `file_ = nullptr`.
- Chỉ khi gói tin đầu tiên của topic đó thực sự xuất hiện trong phân đoạn hiện tại, hàm `ensureFileOpen()` mới được kích hoạt để tạo thư mục và mở file `.dat`.

#### 3. Thuật toán cắt file (300-Second Segmentation Rollover):
Khi một gói tin được rút ra từ hàng đợi, chỉ số phân đoạn được tính toán dựa trên độ chênh lệch thời gian so với mốc bắt đầu phiên ghi:
```cpp
uint64_t segment_duration_us = static_cast<uint64_t>(config_.segment_sec) * 1000000ULL; // 300,000,000 us
uint64_t seg_idx = (msg.timestamp_us >= session_start_us_)
    ? ((msg.timestamp_us - session_start_us_) / segment_duration_us)
    : 0;
uint64_t seg_start_ts = session_start_us_ + seg_idx * segment_duration_us;

if (seg_idx != current_segment_idx) {
    for (auto& [_, writer] : writers_) {
        writer->rollSegment(seg_start_ts); // Đóng phân đoạn cũ
    }
    current_segment_idx = seg_idx;
}
```
Nhờ phép chia nguyên thời gian micro giây, các phân đoạn được cắt đều đặn tuyệt đối 300 giây một lần (`09:26:00 -> 09:31:00 -> 09:36:00...`), không có độ trôi tích lũy (No Cumulative Drift).

---

## 5. Quy trình xử lý bên trong Replay (Step-by-Step)

Khối Replay giải quyết bài toán hóc búa nhất: **Làm sao tái hiện lại trật tự thời gian chính xác giữa 4 luồng dữ liệu độc lập đã bị ghi tách riêng rẽ vào 4 thư mục khác nhau?**

```
                 KIẾN TRÚC BÊN TRONG KHỐI REPLAY
                 
  video/video_*.dat    hit/hit_*.dat      ssr/ssr_*.dat      adsb/adsb_*.dat
         |                  |                  |                  |
         v                  v                  v                  v
   [TopicReader 0]    [TopicReader 1]    [TopicReader 2]    [TopicReader 3]
   (Buffer 128KB)     (Buffer 128KB)     (Buffer 128KB)     (Buffer 128KB)
         \                  \                  /                  /
          \                  \                /                  /
           v                  v              v                  v
     +---------------------------------------------------------------+
     |                   MIN-HEAP (std::priority_queue)              |
     |  Chứa đúng 1 phần tử đại diện từ mỗi Reader còn dữ liệu:      |
     |  HeapItem { msg, reader_idx } sắp xếp theo msg.timestamp_us   |
     +---------------------------------------------------------------+
                                     |
                                     | 1. Pop gói tin có timestamp nhỏ nhất
                                     v
     +---------------------------------------------------------------+
     |              BỘ ĐIỀU NHỊP MICRO GIÂY (HYBRID PACING)          |
     | - delta_us = item.timestamp_us - session_start_ts             |
     | - target_time = real_start + delta_us                         |
     | - Nếu còn > 1500us: std::this_thread::sleep_until(...)        |
     | - Trong 1500us cuối: Spin-wait với _mm_pause()                |
     +---------------------------------------------------------------+
                                     |
                                     | 2. Phát thông điệp 2 phần qua ZMQ PUB
                                     v
     +---------------------------------------------------------------+
     |        ZMQ PUB SOCKET (tcp://192.168.5.31:30004)              |
     |        pub_sock.send([topic], ZMQ_SNDMORE);                   |
     |        pub_sock.send([frame_RDP], 0);                         |
     +---------------------------------------------------------------+
                                     |
                                     | 3. Nạp gói kế tiếp từ Reader đó vào Heap
                                     v
                     readers[item.reader_idx]->getNext()
```

### 5.1 Đọc chuỗi phân đoạn tuần tự với `TopicReader`
Cài đặt trong [`TopicReader.cpp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/replay/TopicReader.cpp):
1. **Tìm kiếm và sắp xếp file**:
   Khi khởi tạo, `TopicReader` dùng `QDir::entryList` lọc theo mẫu `<topic>_*.dat` và sắp xếp theo tên (`QDir::Name`). Vì tên file chứa mốc thời gian `yyyyMMdd_HHmmss`, thứ tự sắp xếp theo tên tự nhiên chính là thứ tự thời gian chuẩn.
2. **Nối file mượt mà (Seamless Rollover)**:
   Trong hàm `getNext()`:
   - Đọc 10 byte header từ file hiện tại.
   - Nếu gặp cuối file (EOF), `TopicReader` tự động đóng file cũ, mở file kế tiếp trong danh sách phân đoạn, và đọc tiếp mà không làm ngắt quãng luồng xử lý.
   - Tự động bắt lỗi nếu file bị cắt cụt (truncated frame) do mất điện đột ngột trong quá trình ghi trước đó.

---

### 5.2 Thuật toán trộn N kênh theo thứ tự thời gian bằng Min-Heap (N-Way Merge)
Nếu chúng ta nạp toàn bộ hàng Gigabyte dữ liệu của tất cả các file vào RAM rồi sắp xếp thì sẽ gây sập bộ nhớ. Do đó, thuật toán **K-Way Merge bằng Min-Heap** được áp dụng với mức tiêu thụ RAM cực thấp và cố định.

Cấu trúc [`HeapItem`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/replay/ReplayEngine.h#L32-L39):
```cpp
struct HeapItem {
    TelemetryMessage msg;
    size_t reader_idx{0};

    // Toán tử so sánh lớn hơn để dựng Min-Heap (phần tử nhỏ nhất nổi lên đỉnh)
    bool operator>(const HeapItem& other) const noexcept {
        return msg.timestamp_us > other.msg.timestamp_us;
    }
};
```

#### Ví dụ minh họa từng bước hoạt động của Heap:
Giả sử ta có 4 kênh dữ liệu với mốc thời gian của các gói tin đầu tiên (đơn vị giây):
- Reader 0 (`video`): $10.002, 10.008, 10.012\dots$
- Reader 1 (`hit`): $10.001, 10.005, 10.009\dots$
- Reader 2 (`ssr`): $10.003, 10.007, 10.011\dots$
- Reader 3 (`adsb`): $10.150, 10.300, 10.450\dots$

```
BƯỚC 1: KHỞI TẠO (PRIME THE HEAP)
Mỗi Reader đọc đúng 1 gói đầu tiên nạp vào Heap:
Heap chứa 4 phần tử:
  [hit  10.001 (idx=1)]  <-- ĐỈNH HEAP (NHỎ NHẤT)
  [video 10.002 (idx=0)]
  [ssr   10.003 (idx=2)]
  [adsb  10.150 (idx=3)]

BƯỚC 2: PHÁT GÓI ĐẦU TIÊN & NẠP BÙ
- Lấy đỉnh Heap: Gói [hit 10.001]. Điều nhịp và bắn ra mạng.
- Đọc gói tiếp theo từ Reader 1 (được gói 10.005) -> Đẩy vào Heap.
Heap tái cấu trúc:
  [video 10.002 (idx=0)]  <-- ĐỈNH HEAP MỚI
  [ssr   10.003 (idx=2)]
  [hit   10.005 (idx=1)]
  [adsb  10.150 (idx=3)]

BƯỚC 3: PHÁT GÓI THỨ HAI & NẠP BÙ
- Lấy đỉnh Heap: Gói [video 10.002]. Điều nhịp và bắn ra mạng.
- Đọc gói tiếp theo từ Reader 0 (được gói 10.008) -> Đẩy vào Heap.
Heap tái cấu trúc:
  [ssr   10.003 (idx=2)]  <-- ĐỈNH HEAP MỚI
  [hit   10.005 (idx=1)]
  [video 10.008 (idx=0)]
  [adsb  10.150 (idx=3)]
```
Cứ như vậy, các gói tin từ 4 luồng hoàn toàn độc lập được phát lại theo đúng trật tự thời gian tuyến tính với độ chính xác tuyệt đối!

---

### 5.3 Kỹ thuật điều nhịp phát lại chính xác đến Micro giây (Hybrid Pacing)

Một bài toán kỹ thuật rất hóc búa: **Làm sao để phát gói tin đúng đến từng micro giây?**

#### Hạn chế của các hàm Sleep thông thường:
Nếu bạn gọi `std::this_thread::sleep_for(std::chrono::microseconds(100))` hoặc dùng `usleep()` trên Linux, bộ lập lịch của hệ điều hành (OS Scheduler) sẽ chuyển luồng của bạn sang trạng thái chờ và nhường CPU cho tiến trình khác.
Đến khi hệ điều hành đánh thức luồng của bạn dậy, thời gian đã trôi qua **từ 1 ms đến 15 ms (tùy thuộc vào xung nhịp ngắt timer interrupt của Windows/Linux)**. Sai số (Jitter) này là không thể chấp nhận được trong hệ thống radar!

#### Giải pháp kết hợp Hybrid Sleep + Spin-wait:
Cài đặt trong [`ReplayEngine.cpp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/replay/ReplayEngine.cpp#L115-L127):

```cpp
// 1. Tính toán thời điểm đích cần phát trên đồng hồ thực tế
uint64_t delta_us = item.msg.timestamp_us - session_start_ts;
auto target_time = real_start + std::chrono::microseconds(delta_us);

// 2. GIAI ĐOẠN 1: Coarse Sleep (Ngủ thô bằng hệ điều hành)
// Nếu thời gian chờ còn dài (> 1500 microseconds), ta cho luồng ngủ để nhường CPU
auto now = std::chrono::steady_clock::now();
if (target_time - now > std::chrono::microseconds(1500)) {
    std::this_thread::sleep_until(target_time - std::chrono::microseconds(1500));
}

// 3. GIAI ĐOẠN 2: Fine Spin-Wait (Chờ bận trong 1500 micro giây cuối)
// Luồng thức dậy sớm hơn đích 1.5ms và bước vào vòng lặp kiểm tra liên tục
while (std::chrono::steady_clock::now() < target_time) {
#if defined(__x86_64__) || defined(_M_X64)
    _mm_pause(); // Lệnh assembly PAUSE của CPU Intel/AMD
#else
    std::this_thread::yield();
#endif
}
```

#### Vai trò kỳ diệu của lệnh `_mm_pause()`:
Lệnh nội tại `_mm_pause()` sinh ra chỉ thị CPU `PAUSE`. Lệnh này có 3 tác dụng then chốt:
1. Thông báo cho CPU biết đây là một Spin-loop (vòng lặp chờ), CPU sẽ tạm dừng pipeline trong khoảng vài chu kỳ clock, ngăn ngừa hiện tượng nghẽn luồng thực thi (Pipeline Stall) khi nhảy ra khỏi vòng lặp.
2. Giảm đáng kể điện năng tiêu thụ và nhiệt độ của nhân CPU so với vòng lặp rỗng `while(true) {}`.
3. Giúp hệ thống đạt độ chính xác điều nhịp **dưới 2 micro giây** mà vẫn không làm treo đứng máy tính!

---

## 6. Cẩm nang xây dựng dự án từ đầu (Implementation Checklist)

Nếu bạn muốn tự tay gõ lại dự án này từ một thư mục trống, hãy làm theo đúng thứ tự các bước dưới đây:

### Bước 1: Khởi tạo Project & Cấu hình `CMakeLists.txt`
1. Đặt chuẩn C++: `set(CMAKE_CXX_STANDARD 20)`.
2. Tìm kiếm các thư viện bắt buộc:
   - `find_package(Qt6 REQUIRED COMPONENTS Core)`
   - `find_library(ZMQ_LIB NAMES zmq ...)`
3. Liên kết các thư viện: `Qt6::Core`, `zmq`, `pthread`.

### Bước 2: Viết tầng dữ liệu chung (`src/common/`)
1. Tạo [`Types.h`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/common/Types.h):
   - Định nghĩa hằng số `SOP_MARKER = 0x0D0D`, `EOP_MARKER = 0x0A0A`, `HEADER_SIZE = 10`.
   - Cấu trúc `struct TelemetryMessage` chứa `topic`, `timestamp_us`, `payload` và phương thức tính kích thước `byteSize()`.
   - Hàm `currentUtcMicroseconds()` lấy giờ micro giây từ `system_clock`.
2. Tạo [`EndianCodec.h`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/common/EndianCodec.h):
   - Hàm `serializeHeader` và `deserializeHeader` sử dụng `qToLittleEndian`/`qToBigEndian`.
   - Hàm `verifyFrameMarkers` kiểm tra SOP và EOP.
   - Hàm `formatSessionTimestamp` (`yyyyMMdd_HHmm`) và `formatSegmentTimestamp` (`yyyyMMdd_HHmmss`).

### Bước 3: Viết khối lưu trữ Recorder (`src/recorder/`)
1. Tạo [`BoundedByteQueue.h`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/BoundedByteQueue.h):
   - Cài đặt hàng đợi an toàn đa luồng giới hạn 256 MB.
   - Viết logic `push()` không block luồng nhận, tự drop khi đầy.
   - Viết logic `drain()` dùng `std::swap` để rút cả lô dữ liệu.
2. Tạo [`TopicWriter.h`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/TopicWriter.h) & [`TopicWriter.cpp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/TopicWriter.cpp):
   - Cài đặt bộ đệm I/O 128 KB với `setvbuf`.
   - Cài đặt cơ chế Lazy Creation trong `ensureFileOpen()`.
   - Cài đặt `rollSegment()` để xoay vòng file sau mỗi 300 giây.
3. Tạo [`RecorderEngine.h`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/RecorderEngine.h) & [`RecorderEngine.cpp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/recorder/RecorderEngine.cpp):
   - Cài đặt `receiverLoop` nhận socket ZMQ SUB với `rcvtimeo = 200ms`.
   - Cài đặt `writerLoop` phân loại gói tin theo topic và ghi xuống đĩa.

### Bước 4: Viết khối phát lại Replay (`src/replay/`)
1. Tạo [`TopicReader.h`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/replay/TopicReader.h) & [`TopicReader.cpp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/replay/TopicReader.cpp):
   - Tìm kiếm danh sách file `.dat` theo thứ tự tên.
   - Đọc tuần tự từng gói tin 10 byte header + payload. Tự động chuyển file kế tiếp khi gặp EOF.
2. Tạo [`ReplayEngine.h`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/replay/ReplayEngine.h) & [`ReplayEngine.cpp`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/src/replay/ReplayEngine.cpp):
   - Khởi tạo socket ZMQ PUB và tạm dừng 1000ms (Slow-Joiner mitigation).
   - Nạp phần tử đầu tiên của các Reader vào `std::priority_queue` (Min-Heap).
   - Vòng lặp phát: Lấy đỉnh Heap -> Điều nhịp Hybrid Pacing -> Gửi multipart ZMQ -> Nạp gói tiếp theo từ Reader đó vào Heap.

### Bước 5: Viết điểm vào chương trình (`src/main.cpp`)
1. Phân tích tham số dòng lệnh CLI (`--mode=record|replay`, `--sub-endpoint`, `--pub-endpoint`, `--segment-sec`, `--endian`...).
2. Bắt tín hiệu hệ điều hành `SIGINT` (`Ctrl+C`) và `SIGTERM` để dừng hệ thống an toàn, xả hết dữ liệu tồn trong hàng đợi xuống đĩa trước khi thoát.

### Bước 6: Kiểm thử xác minh vòng kín (Closed-Loop Verification)
Sau khi biên dịch thành công, chạy kịch bản kiểm thử tự động tại [`tests/run_closed_loop_verification.py`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/tests/run_closed_loop_verification.py):
1. Bắn 2000 gói tin giả lập qua các topic `video`, `hit`, `ssr`, `adsb`.
2. Recorder 1 ghi vào thư mục `session1`.
3. Kiểm tra file nhị phân bằng công cụ [`mcp_zmq_dat_inspector.py`](file:///d:/Xforce/Recoding%20&%20Replaying%20Data%20by%20ZMQ/tools/mcp_zmq_dat_inspector.py) xem SOP/EOP và tính đơn điệu của timestamp có hợp lệ không.
4. Bật Recorder 2 đón ở cổng phát của Replay.
5. Replay đọc dữ liệu từ `session1` và phát ra mạng.
6. Recorder 2 ghi vào thư mục `session2`.
7. Thực hiện so sánh từng byte (Byte-by-byte comparison) giữa `session1` và `session2`. Kết quả phải đạt **100% Exact Match với 0 byte sai lệch**!
