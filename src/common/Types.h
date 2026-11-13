#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <chrono>

namespace recorder {

constexpr uint16_t SOP_MARKER = 0x0D0D;
constexpr uint16_t EOP_MARKER = 0x0A0A;
constexpr size_t HEADER_SIZE = 10; // 8B timestamp + 2B data_len

struct TelemetryMessage {
    std::string topic;
    uint64_t timestamp_us{0};
    std::vector<uint8_t> payload; // frame_RDP

    // Memory footprint calculation for BoundedByteQueue
    size_t byteSize() const noexcept {
        return sizeof(TelemetryMessage) + topic.capacity() + payload.capacity();
    }
};

// Helper to get current UTC microseconds
inline uint64_t currentUtcMicroseconds() {
    auto now = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
}

} // namespace recorder
