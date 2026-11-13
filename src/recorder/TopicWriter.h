#pragma once

#include <string>
#include <vector>
#include <cstdio>
#include <cstdint>
#include <memory>
#include "common/Types.h"

namespace recorder {

class TopicWriter {
public:
    TopicWriter(std::string base_zdata_dir, std::string topic, bool use_big_endian);
    ~TopicWriter();

    // Writes message into current segment file. Lazily opens file on first message.
    bool writeMessage(const TelemetryMessage& msg, uint64_t segment_start_ts);

    // Closes current segment file so that next write will start a new segment file lazily.
    void rollSegment(uint64_t new_segment_start_ts);

    // Flushes and closes file.
    void close();

    const std::string& topic() const noexcept { return topic_; }
    uint64_t packetCount() const noexcept { return packet_count_; }
    uint64_t bytesWritten() const noexcept { return bytes_written_; }

private:
    bool ensureFileOpen(uint64_t segment_start_ts);

    const std::string base_zdata_dir_;
    const std::string topic_;
    const bool use_big_endian_;

    std::FILE* file_{nullptr};
    std::vector<char> io_buffer_;
    uint64_t current_segment_start_ts_{0};
    uint64_t packet_count_{0};
    uint64_t bytes_written_{0};
};

} // namespace recorder
