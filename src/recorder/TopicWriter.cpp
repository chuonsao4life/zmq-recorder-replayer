#include "TopicWriter.h"
#include "common/EndianCodec.h"
#include <QDir>
#include <QString>
#include <iostream>

namespace recorder {

constexpr size_t IO_BUFFER_SIZE = 128 * 1024; // 128 KB buffer

TopicWriter::TopicWriter(std::string base_zdata_dir, std::string topic, bool use_big_endian)
    : base_zdata_dir_(std::move(base_zdata_dir))
    , topic_(std::move(topic))
    , use_big_endian_(use_big_endian)
    , io_buffer_(IO_BUFFER_SIZE)
{
}

TopicWriter::~TopicWriter() {
    close();
}

bool TopicWriter::ensureFileOpen(uint64_t segment_start_ts) {
    if (file_) {
        return true;
    }

    current_segment_start_ts_ = segment_start_ts;
    std::string topic_dir = base_zdata_dir_ + "/" + topic_;
    QDir().mkpath(QString::fromStdString(topic_dir));

    std::string filename = topic_ + "_" + formatSegmentTimestamp(segment_start_ts) + ".dat";
    std::string filepath = topic_dir + "/" + filename;

    file_ = std::fopen(filepath.c_str(), "wb");
    if (!file_) {
        std::cerr << "[TopicWriter][" << topic_ << "] ERROR: Failed to open file: " << filepath << std::endl;
        return false;
    }

    // Set 128 KB buffered file I/O
    if (std::setvbuf(file_, io_buffer_.data(), _IOFBF, io_buffer_.size()) != 0) {
        std::cerr << "[TopicWriter][" << topic_ << "] WARNING: Failed to set 128KB buffer" << std::endl;
    }

    std::cout << "[TopicWriter][" << topic_ << "] Lazily created segment file: " << filepath << std::endl;
    return true;
}

bool TopicWriter::writeMessage(const TelemetryMessage& msg, uint64_t segment_start_ts) {
    if (!ensureFileOpen(segment_start_ts)) {
        return false;
    }

    if (msg.payload.size() > UINT16_MAX) {
        std::cerr << "[TopicWriter][" << topic_ << "] ERROR: Payload exceeds uint16 max" << std::endl;
        return false;
    }

    uint8_t hdr[HEADER_SIZE];
    const auto payload_len = static_cast<uint16_t>(msg.payload.size());
    serializeHeader(msg.timestamp_us, payload_len, hdr, use_big_endian_);

    size_t w_hdr = std::fwrite(hdr, 1, HEADER_SIZE, file_);
    size_t w_data = std::fwrite(msg.payload.data(), 1, msg.payload.size(), file_);

    if (w_hdr != HEADER_SIZE || w_data != msg.payload.size()) {
        std::cerr << "[TopicWriter][" << topic_ << "] ERROR: Write failed for packet" << std::endl;
        return false;
    }

    packet_count_++;
    bytes_written_ += (HEADER_SIZE + msg.payload.size());
    return true;
}

void TopicWriter::rollSegment(uint64_t new_segment_start_ts) {
    if (file_) {
        std::fflush(file_);
        std::fclose(file_);
        file_ = nullptr;
    }
    current_segment_start_ts_ = new_segment_start_ts;
    // Do NOT open file here! Maintain Lazy File Creation: open on next message write.
}

void TopicWriter::close() {
    if (file_) {
        std::fflush(file_);
        std::fclose(file_);
        file_ = nullptr;
    }
}

} // namespace recorder
