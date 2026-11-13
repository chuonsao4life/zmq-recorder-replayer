#pragma once

#include <string>
#include <vector>
#include <atomic>
#include <cstdint>
#include <map>

namespace bridge {

struct BridgeConfig {
    std::string tcp_host{"127.0.0.1"};
    uint16_t tcp_port{8889};
    std::string pub_endpoint{"tcp://192.168.5.21:30003"};
    uint32_t reconnect_interval_ms{1000};
};

class ZmqBridge {
public:
    explicit ZmqBridge(BridgeConfig config);
    ~ZmqBridge();

    bool run();
    void stop();

    void printSummary() const;

private:
    bool connectTcp();
    void disconnectTcp();

    const BridgeConfig config_;
    std::atomic<bool> stop_requested_{false};

    int tcp_sock_{-1};
    std::vector<uint8_t> rx_buffer_;

    std::map<std::string, uint64_t> published_frames_;
    std::map<std::string, uint64_t> published_bytes_;
    uint64_t total_frames_{0};
    uint64_t total_bytes_{0};
};

} // namespace bridge
