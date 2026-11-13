#pragma once

#include <string>
#include <vector>
#include <cstdio>
#include <cstdint>
#include "common/Types.h"

namespace recorder {

class TopicReader {
public:
    TopicReader(std::string topic_dir, std::string topic, bool use_big_endian);
    ~TopicReader();

    bool hasFiles() const noexcept { return !files_.empty(); }

    // Reads the next message from the chronological sequence of .dat files for this topic.
    // Returns true if a packet was successfully read, false on EOF of all files.
    bool getNext(TelemetryMessage& msg);

    const std::string& topic() const noexcept { return topic_; }
    uint64_t totalPacketsRead() const noexcept { return total_packets_read_; }
    uint64_t totalBytesRead() const noexcept { return total_bytes_read_; }

private:
    bool openNextFile();

    const std::string topic_dir_;
    const std::string topic_;
    const bool use_big_endian_;

    std::vector<std::string> files_;
    size_t current_file_idx_{0};
    std::FILE* current_file_{nullptr};
    std::vector<char> io_buffer_;

    uint64_t total_packets_read_{0};
    uint64_t total_bytes_read_{0};
};

} // namespace recorder
