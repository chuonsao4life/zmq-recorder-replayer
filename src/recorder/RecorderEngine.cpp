#include "RecorderEngine.h"
#include "common/EndianCodec.h"
#include <zmq.hpp>
#include <iostream>
#include <iomanip>
#include <QDir>

namespace recorder {

RecorderEngine::RecorderEngine(RecorderConfig config)
    : config_(std::move(config))
    , queue_(std::make_unique<BoundedByteQueue>(config_.max_queue_mb * 1024ULL * 1024ULL))
{
}

RecorderEngine::~RecorderEngine() {
    stop();
    wait();
}

bool RecorderEngine::start() {
    if (running_.exchange(true)) {
        return false;
    }

    session_start_us_ = currentUtcMicroseconds();
    session_dir_ = config_.out_dir + "/" + formatSessionTimestamp(session_start_us_);
    zdata_dir_ = session_dir_ + "/zdata";
    QDir().mkpath(QString::fromStdString(zdata_dir_));

    std::cout << "===========================================================" << std::endl;
    std::cout << "[RecorderEngine] Starting Recorder Engine" << std::endl;
    std::cout << "  Endpoint     : " << config_.sub_endpoint << std::endl;
    std::cout << "  Session Dir  : " << session_dir_ << std::endl;
    std::cout << "  ZData Dir    : " << zdata_dir_ << std::endl;
    std::cout << "  Segment Sec  : " << config_.segment_sec << " s" << std::endl;
    std::cout << "  Max Queue MB : " << config_.max_queue_mb << " MB" << std::endl;
    std::cout << "  Endianness   : " << (config_.use_big_endian ? "Big-Endian" : "Little-Endian") << std::endl;
    std::cout << "  Topics       : ";
    for (const auto& t : config_.topics) std::cout << t << " ";
    std::cout << "\n===========================================================" << std::endl;

    writer_thread_ = std::thread(&RecorderEngine::writerLoop, this);
    receiver_thread_ = std::thread(&RecorderEngine::receiverLoop, this);

    return true;
}

void RecorderEngine::stop() {
    if (!running_) return;
    stop_requested_ = true;
    if (queue_) {
        queue_->requestStop();
    }
}

void RecorderEngine::wait() {
    if (receiver_thread_.joinable()) {
        receiver_thread_.join();
    }
    if (writer_thread_.joinable()) {
        writer_thread_.join();
    }
    running_ = false;
}

void RecorderEngine::receiverLoop() {
    zmq::context_t ctx{1};
    zmq::socket_t sub_sock{ctx, zmq::socket_type::sub};

    try {
        sub_sock.set(zmq::sockopt::rcvtimeo, 200); // 200ms timeout for clean exit
        sub_sock.connect(config_.sub_endpoint);

        for (const auto& topic : config_.topics) {
            sub_sock.set(zmq::sockopt::subscribe, topic);
        }
        std::cout << "[ReceiverThread] Connected to " << config_.sub_endpoint << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "[ReceiverThread] ERROR connecting: " << e.what() << std::endl;
        stop();
        return;
    }

    auto start_time = std::chrono::steady_clock::now();

    while (!stop_requested_) {
        if (config_.duration_sec > 0) {
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::steady_clock::now() - start_time).count();
            if (elapsed >= config_.duration_sec) {
                std::cout << "[ReceiverThread] Duration reached (" << config_.duration_sec << "s). Stopping..." << std::endl;
                stop();
                break;
            }
        }

        zmq::message_t topic_msg;
        auto res = sub_sock.recv(topic_msg, zmq::recv_flags::none);
        if (!res) {
            continue; // Timeout, check stop condition
        } 

        uint64_t recv_ts = currentUtcMicroseconds();

        if (!topic_msg.more()) {    
            total_invalid_++;
            continue;
        }

        zmq::message_t payload_msg;
        auto res_payload = sub_sock.recv(payload_msg, zmq::recv_flags::none);
        if (!res_payload) {
            total_invalid_++;
            continue;
        }

        const uint8_t* payload_ptr = static_cast<const uint8_t*>(payload_msg.data());
        size_t payload_len = payload_msg.size();

        if (!verifyFrameMarkers(payload_ptr, payload_len, config_.use_big_endian)) {
            total_invalid_++;
            continue;
        }

        TelemetryMessage msg;
        msg.topic = topic_msg.to_string();
        msg.timestamp_us = recv_ts;
        msg.payload.assign(payload_ptr, payload_ptr + payload_len);

        queue_->push(std::move(msg));
        total_received_++;
    }

    sub_sock.close();
    ctx.close();
    std::cout << "[ReceiverThread] Exited." << std::endl;
}

void RecorderEngine::writerLoop() {
    uint64_t segment_duration_us = static_cast<uint64_t>(config_.segment_sec) * 1000000ULL;
    uint64_t current_segment_idx = 0;

    std::deque<TelemetryMessage> batch;

    while (true) {
        bool drained = queue_->drain(batch, std::chrono::milliseconds(50));
        if (drained) {
            for (const auto& msg : batch) {
                uint64_t seg_idx = (msg.timestamp_us >= session_start_us_)
                    ? ((msg.timestamp_us - session_start_us_) / segment_duration_us)
                    : 0;
                uint64_t seg_start_ts = session_start_us_ + seg_idx * segment_duration_us;

                if (seg_idx != current_segment_idx) {
                    for (auto& [_, writer] : writers_) {
                        writer->rollSegment(seg_start_ts);
                    }
                    current_segment_idx = seg_idx;
                }

                auto it = writers_.find(msg.topic);
                if (it == writers_.end()) {
                    auto w = std::make_unique<TopicWriter>(zdata_dir_, msg.topic, config_.use_big_endian);
                    it = writers_.emplace(msg.topic, std::move(w)).first;
                }

                it->second->writeMessage(msg, seg_start_ts);
            }
        }

        // On stop: keep draining until the queue is truly empty so no messages are lost.
        if (stop_requested_ && !drained && queue_->currentBytes() == 0) {
            break;
        }
    }

    for (auto& [_, writer] : writers_) {
        writer->close();
    }

    std::cout << "[WriterThread] Exited." << std::endl;
}

void RecorderEngine::printSummary() const {
    std::cout << "\n===========================================================" << std::endl;
    std::cout << "                 RECORDER SESSION SUMMARY                  " << std::endl;
    std::cout << "===========================================================" << std::endl;
    std::cout << "  Session Directory : " << session_dir_ << std::endl;
    std::cout << "  Total Received    : " << total_received_.load() << std::endl;
    std::cout << "  Total Invalid     : " << total_invalid_.load() << std::endl;
    std::cout << "  Queue Dropped     : " << queue_->droppedMessages() << " pkts ("
              << (queue_->droppedBytes() / 1024.0 / 1024.0) << " MB)" << std::endl;
    std::cout << "-----------------------------------------------------------" << std::endl;
    std::cout << std::left << std::setw(12) << "Topic"
              << std::setw(16) << "Packets Recorded"
              << std::setw(16) << "Bytes Written" << std::endl;
    std::cout << "-----------------------------------------------------------" << std::endl;

    uint64_t grand_pkts = 0;
    uint64_t grand_bytes = 0;

    for (const auto& [topic, writer] : writers_) {
        std::cout << std::left << std::setw(12) << topic
                  << std::setw(16) << writer->packetCount()
                  << std::setw(16) << writer->bytesWritten() << std::endl;
        grand_pkts += writer->packetCount();
        grand_bytes += writer->bytesWritten();
    }
    std::cout << "-----------------------------------------------------------" << std::endl;
    std::cout << std::left << std::setw(12) << "TOTAL"
              << std::setw(16) << grand_pkts
              << std::setw(16) << grand_bytes << std::endl;
    std::cout << "===========================================================\n" << std::endl;
}

} // namespace recorder
