#include "ZmqBridge.h"
#include "zmq.hpp"

#include <iostream>
#include <iomanip>
#include <thread>
#include <chrono>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <poll.h>

namespace bridge {

ZmqBridge::ZmqBridge(BridgeConfig config)
    : config_(std::move(config))
{
    rx_buffer_.reserve(64 * 1024);
}

ZmqBridge::~ZmqBridge() {
    stop();
}

void ZmqBridge::stop() {
    stop_requested_ = true;
    disconnectTcp();
}

void ZmqBridge::disconnectTcp() {
    if (tcp_sock_ >= 0) {
        ::close(tcp_sock_);
        tcp_sock_ = -1;
    }
}

bool ZmqBridge::connectTcp() {
    disconnectTcp();

    int sock = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        return false;
    }

    // Set non-blocking during connect to support timeout / stop interrupt
    int flags = ::fcntl(sock, F_GETFL, 0);
    ::fcntl(sock, F_SETFL, flags | O_NONBLOCK);

    struct sockaddr_in serv_addr;
    std::memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(config_.tcp_port);

    if (::inet_pton(AF_INET, config_.tcp_host.c_str(), &serv_addr.sin_addr) <= 0) {
        ::close(sock);
        return false;
    }

    int res = ::connect(sock, reinterpret_cast<struct sockaddr*>(&serv_addr), sizeof(serv_addr));
    if (res < 0 && errno != EINPROGRESS) {
        ::close(sock);
        return false;
    }

    struct pollfd pfd;
    pfd.fd = sock;
    pfd.events = POLLOUT;

    int poll_res = ::poll(&pfd, 1, 1000); // 1s timeout for connection
    if (poll_res <= 0) {
        ::close(sock);
        return false;
    }

    int err = 0;
    socklen_t err_len = sizeof(err);
    if (::getsockopt(sock, SOL_SOCKET, SO_ERROR, &err, &err_len) < 0 || err != 0) {
        ::close(sock);
        return false;
    }

    // Switch back to normal blocking with poll-based reads
    ::fcntl(sock, F_SETFL, flags);
    tcp_sock_ = sock;
    return true;
}

bool ZmqBridge::run() {
    std::cout << "===========================================================" << std::endl;
    std::cout << "[ZmqBridge] Starting ZMQ Bridge Engine" << std::endl;
    std::cout << "  TCP Source       : " << config_.tcp_host << ":" << config_.tcp_port << std::endl;
    std::cout << "  Publish Endpoint : " << config_.pub_endpoint << std::endl;
    std::cout << "  Auto-Reconnect   : " << config_.reconnect_interval_ms << " ms" << std::endl;
    std::cout << "===========================================================" << std::endl;

    zmq::context_t ctx{1};
    zmq::socket_t pub_sock{ctx, zmq::socket_type::pub};
    pub_sock.set(zmq::sockopt::linger, 500);

    try {
        pub_sock.bind(config_.pub_endpoint);
        std::cout << "[ZmqBridge] Bound ZMQ PUB socket to " << config_.pub_endpoint << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "[ZmqBridge] ERROR binding PUB endpoint: " << e.what() << std::endl;
        return false;
    }

    uint8_t read_buf[4096];

    while (!stop_requested_) {
        // Connect / Reconnect to TCP source
        if (tcp_sock_ < 0) {
            std::cout << "[ZmqBridge] Connecting to TCP " << config_.tcp_host << ":" << config_.tcp_port << "..." << std::endl;
            if (!connectTcp()) {
                // Sleep reconnect interval in small chunks to check stop_requested_
                for (uint32_t ms = 0; ms < config_.reconnect_interval_ms && !stop_requested_; ms += 100) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
                continue;
            }
            std::cout << "[ZmqBridge] Successfully connected to TCP source "
                      << config_.tcp_host << ":" << config_.tcp_port << std::endl;
            rx_buffer_.clear();
        }

        // Poll TCP socket for incoming bytes
        struct pollfd pfd;
        pfd.fd = tcp_sock_;
        pfd.events = POLLIN;

        int ret = ::poll(&pfd, 1, 200); // 200ms poll timeout
        if (ret < 0) {
            if (errno == EINTR) continue;
            std::cerr << "[ZmqBridge] Poll error: " << std::strerror(errno) << std::endl;
            disconnectTcp();
            continue;
        }

        if (ret == 0) {
            // Timeout, loop back to check stop_requested_
            continue;
        }

        if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
            std::cout << "[ZmqBridge] TCP socket hangup / error." << std::endl;
            disconnectTcp();
            continue;
        }

        if (pfd.revents & POLLIN) {
            ssize_t n = ::recv(tcp_sock_, read_buf, sizeof(read_buf), 0);
            if (n <= 0) {
                std::cout << "[ZmqBridge] TCP peer disconnected." << std::endl;
                disconnectTcp();
                continue;
            }

            // Append received chunk to stream framing accumulation buffer
            rx_buffer_.insert(rx_buffer_.end(), read_buf, read_buf + n);

            // Stream framing state machine:
            // Process all complete RDP frames in rx_buffer_
            while (rx_buffer_.size() >= 12) {
                // 1. Search for SOP marker (0x0D0D in Little-Endian)
                uint16_t sop = static_cast<uint16_t>(rx_buffer_[0]) |
                               (static_cast<uint16_t>(rx_buffer_[1]) << 8);

                if (sop != 0x0D0D) {
                    // Resynchronize: drop 1 byte to find next SOP
                    rx_buffer_.erase(rx_buffer_.begin());
                    continue;
                }

                // 2. Read LEN field (offset 6-7, Little-Endian)
                uint16_t payload_len = static_cast<uint16_t>(rx_buffer_[6]) |
                                       (static_cast<uint16_t>(rx_buffer_[7]) << 8);

                size_t total_frame_len = static_cast<size_t>(payload_len) + 12;

                if (rx_buffer_.size() < total_frame_len) {
                    // Fragmented frame: need more bytes, wait for next recv
                    break;
                }

                // 3. Verify EOP marker (offset total_frame_len - 2, Little-Endian)
                uint16_t eop = static_cast<uint16_t>(rx_buffer_[total_frame_len - 2]) |
                               (static_cast<uint16_t>(rx_buffer_[total_frame_len - 1]) << 8);

                if (eop != 0x0A0A) {
                    // False SOP or corrupted frame: discard false SOP and search again
                    rx_buffer_.erase(rx_buffer_.begin() + 2);
                    continue;
                }

                // 4. Map TYPE (offset 5) to topic
                uint8_t type_byte = rx_buffer_[5];
                std::string topic;
                switch (type_byte) {
                    case 0x01: topic = "video"; break;
                    case 0x02: topic = "hit"; break;
                    case 0x10:
                    case 0x11: topic = "ssr"; break;
                    case 0xF1: topic = "adsb"; break;
                    default:
                        // Unknown type: discard frame
                        break;
                }

                if (!topic.empty()) {
                    // Send 2-part message: [topic (ASCII, no null)][frame_RDP (raw)]
                    zmq::message_t topic_msg(topic.data(), topic.size());
                    zmq::message_t payload_msg(rx_buffer_.data(), total_frame_len);

                    pub_sock.send(topic_msg, zmq::send_flags::sndmore);
                    pub_sock.send(payload_msg, zmq::send_flags::none);

                    published_frames_[topic]++;
                    published_bytes_[topic] += total_frame_len;
                    total_frames_++;
                    total_bytes_ += total_frame_len;
                }

                // Remove processed frame from accumulation buffer
                rx_buffer_.erase(rx_buffer_.begin(), rx_buffer_.begin() + total_frame_len);
            }
        }
    }

    disconnectTcp();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    pub_sock.close();
    ctx.close();

    printSummary();
    return true;
}

void ZmqBridge::printSummary() const {
    std::cout << "\n===========================================================" << std::endl;
    std::cout << "                  ZMQ BRIDGE SESSION SUMMARY               " << std::endl;
    std::cout << "===========================================================" << std::endl;
    std::cout << "  Total Frames Published : " << total_frames_ << std::endl;
    std::cout << "  Total Payload Bytes    : " << total_bytes_ << " bytes ("
              << (total_bytes_ / 1024.0 / 1024.0) << " MB)" << std::endl;
    std::cout << "-----------------------------------------------------------" << std::endl;
    std::cout << std::left << std::setw(12) << "Topic"
              << std::setw(18) << "Frames Published"
              << std::setw(16) << "Bytes Sent" << std::endl;
    std::cout << "-----------------------------------------------------------" << std::endl;

    for (const auto& [topic, count] : published_frames_) {
        uint64_t bytes = 0;
        auto it = published_bytes_.find(topic);
        if (it != published_bytes_.end()) bytes = it->second;

        std::cout << std::left << std::setw(12) << topic
                  << std::setw(18) << count
                  << std::setw(16) << bytes << std::endl;
    }
    std::cout << "===========================================================\n" << std::endl;
}

} // namespace bridge
