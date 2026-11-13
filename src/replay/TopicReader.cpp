#include "TopicReader.h"
#include "common/EndianCodec.h"
#include <QDir>
#include <QStringList>
#include <iostream>

namespace recorder {

constexpr size_t IO_BUFFER_SIZE = 128 * 1024; // 128 KB buffer

TopicReader::TopicReader(std::string topic_dir, std::string topic, bool use_big_endian)
    : topic_dir_(std::move(topic_dir))
    , topic_(std::move(topic))
    , use_big_endian_(use_big_endian)
    , io_buffer_(IO_BUFFER_SIZE)
{
    QDir dir(QString::fromStdString(topic_dir_));
    if (dir.exists()) {
        QString pattern = QString::fromStdString(topic_ + "_*.dat");
        QStringList file_list = dir.entryList({pattern}, QDir::Files, QDir::Name);
        for (const auto& f : file_list) {
            files_.push_back(dir.absoluteFilePath(f).toStdString());
        }
    }
}

TopicReader::~TopicReader() {
    if (current_file_) {
        std::fclose(current_file_);
        current_file_ = nullptr;
    }
}

bool TopicReader::openNextFile() {
    if (current_file_) {
        std::fclose(current_file_);
        current_file_ = nullptr;
    }

    // Loop so a single unreadable file doesn't stall the whole topic
    while (current_file_idx_ < files_.size()) {
        const std::string& path = files_[current_file_idx_];
        current_file_ = std::fopen(path.c_str(), "rb");
        current_file_idx_++;
        if (!current_file_) {
            std::cerr << "[TopicReader][" << topic_ << "] ERROR: Failed to open " << path
                      << " — skipping." << std::endl;
            continue;
        }
        std::setvbuf(current_file_, io_buffer_.data(), _IOFBF, io_buffer_.size());
        return true;
    }
    return false;  // All files exhausted
}

bool TopicReader::getNext(TelemetryMessage& msg) {
    while (true) {
        if (!current_file_) {
            if (!openNextFile()) {
                return false; // All files for this topic exhausted
            }
        }

        uint8_t hdr[HEADER_SIZE];
        size_t r_hdr = std::fread(hdr, 1, HEADER_SIZE, current_file_);
        if (r_hdr == 0) {
            // EOF of current segment file
            std::fclose(current_file_);
            current_file_ = nullptr;
            continue;
        }

        if (r_hdr < HEADER_SIZE) {
            std::cerr << "[TopicReader][" << topic_ << "] WARNING: Truncated header encountered" << std::endl;
            std::fclose(current_file_);
            current_file_ = nullptr;
            continue;
        }

        uint64_t ts = 0;
        uint16_t data_len = 0;
        deserializeHeader(hdr, ts, data_len, use_big_endian_);

        msg.payload.resize(data_len);
        size_t r_data = std::fread(msg.payload.data(), 1, data_len, current_file_);
        if (r_data < data_len) {
            std::cerr << "[TopicReader][" << topic_ << "] WARNING: Truncated frame encountered" << std::endl;
            std::fclose(current_file_);
            current_file_ = nullptr;
            continue;
        }

        msg.topic = topic_;
        msg.timestamp_us = ts;

        total_packets_read_++;
        total_bytes_read_ += (HEADER_SIZE + data_len);
        return true;
    }
}

} // namespace recorder
