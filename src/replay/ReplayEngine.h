#pragma once

#include <string>
#include <vector>
#include <memory>
#include <atomic>
#include <map>
#include "TopicReader.h"

namespace recorder {

struct ReplayConfig {
    std::string session_dir; // points to <out_dir>/<yyyyMMdd_hhmm>/zdata
    std::string pub_endpoint{"tcp://192.168.5.31:30004"};
    std::vector<std::string> topics{"video", "hit", "ssr", "adsb"};
    bool use_big_endian{false};
    uint32_t slow_joiner_ms{1000};
    uint32_t duration_sec{0};
};

class ReplayEngine {
public:
    explicit ReplayEngine(ReplayConfig config);
    ~ReplayEngine();

    bool run();
    void stop();

    void printSummary() const;

private:
    struct HeapItem {
        TelemetryMessage msg;
        size_t reader_idx{0};

        bool operator>(const HeapItem& other) const noexcept {
            return msg.timestamp_us > other.msg.timestamp_us;
        }
    };

    const ReplayConfig config_;
    std::atomic<bool> stop_requested_{false};

    std::vector<std::unique_ptr<TopicReader>> readers_;
    std::map<std::string, uint64_t> published_counts_;
    std::map<std::string, uint64_t> published_bytes_;

    uint64_t total_published_packets_{0};
    uint64_t total_published_bytes_{0};
    double total_elapsed_sec_{0.0};
};

} // namespace recorder
