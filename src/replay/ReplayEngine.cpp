#include "ReplayEngine.h"
#include "common/EndianCodec.h"
#include <zmq.hpp>
#include <queue>
#include <iostream>
#include <iomanip>
#include <thread>
#include <chrono>
#include <QDateTime>
#include <QTimeZone>

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#endif

namespace recorder {

namespace {

const char* getTopicColor(const std::string& topic) {
    if (topic == "video") return "\033[96m"; // Cyan
    if (topic == "hit")   return "\033[92m"; // Green
    if (topic == "ssr")   return "\033[93m"; // Yellow
    if (topic == "adsb")  return "\033[95m"; // Magenta
    return "\033[97m";
}

const char* getTypeName(uint8_t type) {
    switch (type) {
        case 0x01: return "Radar Raw";
        case 0x02: return "Target Det";
        case 0x10: return "SSR Mode 3";
        case 0x11: return "SSR Mode C";
        case 0xF1: return "ADS-B 1090";
        default:   return "Unknown";
    }
}

void printReplayedPacket(uint64_t count, const TelemetryMessage& msg, bool use_big_endian) {
    // 1. Format timestamp to UTC string: YYYY-MM-DD HH:MM:SS.ffffff UTC
    qint64 ms = static_cast<qint64>(msg.timestamp_us / 1000ULL);
    uint32_t rem_us = static_cast<uint32_t>(msg.timestamp_us % 1000ULL);
    QDateTime dt = QDateTime::fromMSecsSinceEpoch(ms, QTimeZone::utc());
    char us_buf[8];
    std::snprintf(us_buf, sizeof(us_buf), "%03u", rem_us);
    std::string time_str = dt.toString(QStringLiteral("yyyy-MM-dd HH:mm:ss.zzz")).toStdString() + us_buf + " UTC";

    // 2. Parse frame_RDP fields
    const uint8_t* p = msg.payload.data();
    size_t plen = msg.payload.size();

    uint16_t seq = 0;
    uint8_t src_id = 0;
    uint8_t type = 0;
    uint16_t data_len = 0;
    bool has_rdp = (plen >= 12);

    if (has_rdp) {
        seq = use_big_endian ? qFromBigEndian<uint16_t>(p + 2) : qFromLittleEndian<uint16_t>(p + 2);
        src_id = p[4];
        type = p[5];
        data_len = use_big_endian ? qFromBigEndian<uint16_t>(p + 6) : qFromLittleEndian<uint16_t>(p + 6);
    }

    // 3. Data Hex Preview (first 6 bytes)
    std::string hex_preview;
    if (has_rdp && plen > 12) {
        const uint8_t* data_start = p + 8;
        size_t available_data = plen - 12;
        size_t show_n = std::min(available_data, size_t(6));
        for (size_t i = 0; i < show_n; ++i) {
            char hb[4];
            std::snprintf(hb, sizeof(hb), "%02X ", data_start[i]);
            hex_preview += hb;
        }
        if (available_data > show_n) {
            hex_preview += "...";
        }
    } else {
        hex_preview = "<empty>";
    }

    // 4. Formatted string construction
    char src_buf[16];
    std::snprintf(src_buf, sizeof(src_buf), "0x%02X(%u)", src_id, src_id);

    char type_buf[24];
    std::snprintf(type_buf, sizeof(type_buf), "0x%02X %s", type, getTypeName(type));

    std::string len_str = std::to_string(has_rdp ? data_len : plen) + " B";

    const char* col = getTopicColor(msg.topic);
    const char* rst = "\033[0m";
    const char* dim = "\033[2m";
    const char* bold = "\033[1m";

    std::cout << "│ " << std::right << std::setw(5) << count << " │ "
              << dim << std::left << std::setw(26) << time_str << rst << " │ "
              << col << bold << std::left << std::setw(5) << msg.topic << rst << " │ "
              << "\033[96m" << std::left << std::setw(5) << seq << rst << " │ "
              << std::left << std::setw(6) << src_buf << " │ "
              << std::left << std::setw(14) << type_buf << " │ "
              << std::right << std::setw(8) << len_str << " │ "
              << std::left << std::setw(22) << hex_preview << " │" << std::endl;
}

} // anonymous namespace

ReplayEngine::ReplayEngine(ReplayConfig config)
    : config_(std::move(config))
{
}

ReplayEngine::~ReplayEngine() {
    stop();
}

void ReplayEngine::stop() {
    stop_requested_ = true;
}

bool ReplayEngine::run() {
    std::cout << "===========================================================" << std::endl;
    std::cout << "[ReplayEngine] Starting Replay Engine" << std::endl;
    std::cout << "  Session Dir      : " << config_.session_dir << std::endl;
    std::cout << "  Publish Endpoint : " << config_.pub_endpoint << std::endl;
    std::cout << "  Slow-Joiner Delay: " << config_.slow_joiner_ms << " ms" << std::endl;
    std::cout << "  Endianness       : " << (config_.use_big_endian ? "Big-Endian" : "Little-Endian") << std::endl;
    std::cout << "  Topics           : ";
    for (const auto& t : config_.topics) std::cout << t << " ";
    std::cout << "\n===========================================================" << std::endl;

    readers_.clear();
    for (const auto& topic : config_.topics) {
        std::string topic_path = config_.session_dir + "/" + topic;
        auto reader = std::make_unique<TopicReader>(topic_path, topic, config_.use_big_endian);
        if (reader->hasFiles()) {
            std::cout << "[ReplayEngine] Found files for topic: " << topic << std::endl;
            readers_.push_back(std::move(reader));
        } else {
            std::cout << "[ReplayEngine] No .dat files found for topic: " << topic << " at " << topic_path << std::endl;
        }
    }

    if (readers_.empty()) {
        std::cerr << "[ReplayEngine] ERROR: No topic data found in " << config_.session_dir << std::endl;
        return false;
    }

    zmq::context_t ctx{1};
    zmq::socket_t pub_sock{ctx, zmq::socket_type::pub};
    pub_sock.set(zmq::sockopt::linger, 500);

    try {
        pub_sock.bind(config_.pub_endpoint);
        std::cout << "[ReplayEngine] Bound PUB socket to " << config_.pub_endpoint << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "[ReplayEngine] ERROR binding to " << config_.pub_endpoint << ": " << e.what() << std::endl;
        return false;
    }

    std::cout << "[ReplayEngine] Waiting " << config_.slow_joiner_ms
              << " ms for subscribers to connect (slow-joiner mitigation)..." << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(config_.slow_joiner_ms));

    // Priority Queue: Min-Heap ordered by msg.timestamp_us
    std::priority_queue<HeapItem, std::vector<HeapItem>, std::greater<HeapItem>> min_heap;

    // Prime the heap with the first packet from each active topic reader
    for (size_t i = 0; i < readers_.size(); ++i) {
        TelemetryMessage msg;
        if (readers_[i]->getNext(msg)) {
            min_heap.push(HeapItem{std::move(msg), i});
        }
    }

    if (min_heap.empty()) {
        std::cerr << "[ReplayEngine] No messages to replay." << std::endl;
        pub_sock.close();
        ctx.close();
        return false;
    }

    auto real_start = std::chrono::steady_clock::now();
    uint64_t session_start_ts = min_heap.top().msg.timestamp_us;

    std::cout << "[ReplayEngine] Playback started. Initial timestamp: " << session_start_ts << " us\n" << std::endl;
    std::cout << "┌───────┬────────────────────────────┬───────┬───────┬────────┬────────────────┬──────────┬────────────────────────┐" << std::endl;
    std::cout << "│  STT  │ THỜI GIAN PHÁT (UTC)       │ KÊNH  │  SEQ  │ TRẠM ID│ TYPE / Ý NGHĨA │ PAYLOAD  │ PREVIEW DỮ LIỆU (HEX)  │" << std::endl;
    std::cout << "├───────┼────────────────────────────┼───────┼───────┼────────┼────────────────┼──────────┼────────────────────────┤" << std::endl;

    while (!min_heap.empty() && !stop_requested_) {
        // Check duration timeout if configured
        if (config_.duration_sec > 0) {
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::steady_clock::now() - real_start).count();
            if (elapsed >= config_.duration_sec) {
                std::cout << "[ReplayEngine] Replay duration limit (" << config_.duration_sec << "s) reached." << std::endl;
                break;
            }
        }

        HeapItem item = std::move(const_cast<HeapItem&>(min_heap.top()));
        min_heap.pop();

        uint64_t delta_us = (item.msg.timestamp_us >= session_start_ts)
            ? (item.msg.timestamp_us - session_start_ts)
            : 0;

        auto target_time = real_start + std::chrono::microseconds(delta_us);

        // Hybrid Microsecond Replay Pacing
        auto now = std::chrono::steady_clock::now();
        if (target_time - now > std::chrono::microseconds(1500)) {
            std::this_thread::sleep_until(target_time - std::chrono::microseconds(1500));
        }

        while (std::chrono::steady_clock::now() < target_time) {
#if defined(__x86_64__) || defined(_M_X64)
            _mm_pause();
#else
            std::this_thread::yield();
#endif
        }

        // Publish 2-part ZeroMQ message: [topic][frame_RDP]
        zmq::message_t topic_msg(item.msg.topic.data(), item.msg.topic.size());
        zmq::message_t payload_msg(item.msg.payload.data(), item.msg.payload.size());

        pub_sock.send(topic_msg, zmq::send_flags::sndmore);
        pub_sock.send(payload_msg, zmq::send_flags::none);

        published_counts_[item.msg.topic]++;
        published_bytes_[item.msg.topic] += (item.msg.topic.size() + item.msg.payload.size());
        total_published_packets_++;
        total_published_bytes_ += (item.msg.topic.size() + item.msg.payload.size());

        // In chi tiết gói tin vừa phát ra màn hình dưới dạng bảng dễ nhìn
        printReplayedPacket(total_published_packets_, item.msg, config_.use_big_endian);

        // Replenish the heap with next packet from the same reader
        TelemetryMessage next_msg;
        if (readers_[item.reader_idx]->getNext(next_msg)) {
            min_heap.push(HeapItem{std::move(next_msg), item.reader_idx});
        }
    }

    std::cout << "└───────┴────────────────────────────┴───────┴───────┴────────┴────────────────┴──────────┴────────────────────────┘" << std::endl;

    auto real_end = std::chrono::steady_clock::now();
    total_elapsed_sec_ = std::chrono::duration<double>(real_end - real_start).count();

    // Small delay before closing to ensure wire transmission drains
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    pub_sock.close();
    ctx.close();

    printSummary();
    return true;
}

void ReplayEngine::printSummary() const {
    std::cout << "\n===========================================================" << std::endl;
    std::cout << "                  REPLAY SESSION SUMMARY                   " << std::endl;
    std::cout << "===========================================================" << std::endl;
    std::cout << "  Duration         : " << std::fixed << std::setprecision(3) << total_elapsed_sec_ << " s" << std::endl;
    std::cout << "  Total Packets    : " << total_published_packets_ << std::endl;
    std::cout << "  Total Wire Bytes : " << total_published_bytes_ << " bytes ("
              << (total_published_bytes_ / 1024.0 / 1024.0) << " MB)" << std::endl;
    double throughput = (total_elapsed_sec_ > 0) ? (total_published_packets_ / total_elapsed_sec_) : 0.0;
    std::cout << "  Throughput       : " << throughput << " pkts/sec" << std::endl;
    std::cout << "-----------------------------------------------------------" << std::endl;
    std::cout << std::left << std::setw(12) << "Topic"
              << std::setw(16) << "Packets Sent"
              << std::setw(16) << "Bytes Sent" << std::endl;
    std::cout << "-----------------------------------------------------------" << std::endl;

    for (const auto& [topic, count] : published_counts_) {
        uint64_t bytes = 0;
        auto it = published_bytes_.find(topic);
        if (it != published_bytes_.end()) bytes = it->second;

        std::cout << std::left << std::setw(12) << topic
                  << std::setw(16) << count
                  << std::setw(16) << bytes << std::endl;
    }
    std::cout << "===========================================================\n" << std::endl;
}

} // namespace recorder
