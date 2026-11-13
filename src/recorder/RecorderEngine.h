#pragma once

#include <string>
#include <vector>
#include <map>
#include <memory>
#include <thread>
#include <atomic>
#include "BoundedByteQueue.h"
#include "TopicWriter.h"

namespace recorder {

struct RecorderConfig {
    std::string sub_endpoint{"tcp://192.168.5.21:30003"};
    std::string out_dir{"."};
    std::vector<std::string> topics{"video", "hit", "ssr", "adsb"};
    uint32_t segment_sec{300};
    size_t max_queue_mb{256};
    bool use_big_endian{false};
    uint32_t duration_sec{0}; // 0 = run until SIGINT/SIGTERM
};

class RecorderEngine {
public:
    explicit RecorderEngine(RecorderConfig config);
    ~RecorderEngine();

    bool start();
    void stop();
    void wait();

    void printSummary() const;

private:
    void receiverLoop();
    void writerLoop();

    const RecorderConfig config_;
    std::unique_ptr<BoundedByteQueue> queue_;

    std::atomic<bool> running_{false};
    std::atomic<bool> stop_requested_{false};

    std::thread receiver_thread_;
    std::thread writer_thread_;

    uint64_t session_start_us_{0};
    std::string session_dir_;
    std::string zdata_dir_;

    std::map<std::string, std::unique_ptr<TopicWriter>> writers_;

    std::atomic<uint64_t> total_received_{0};
    std::atomic<uint64_t> total_invalid_{0};
};

} // namespace recorder
