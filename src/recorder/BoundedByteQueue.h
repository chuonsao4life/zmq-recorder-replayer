#pragma once

#include "common/Types.h"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>

namespace recorder {

class BoundedByteQueue {
public:
  explicit BoundedByteQueue(size_t max_bytes = 256ULL * 1024ULL * 1024ULL)
      : max_bytes_(max_bytes) {}

  ~BoundedByteQueue() { requestStop(); }

  // Move message into queue. Drops immediately if queue would exceed max_bytes.
  // Never blocks the Receiver thread.
  bool push(TelemetryMessage &&msg) {
    const size_t msg_size = msg.byteSize();
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (stop_requested_.load(std::memory_order_acquire)) {
        return false;
      }

      if (current_bytes_ + msg_size > max_bytes_) {
        dropped_messages_.fetch_add(1, std::memory_order_relaxed);
        dropped_bytes_.fetch_add(msg_size, std::memory_order_relaxed);
        return false;
      }

      current_bytes_ += msg_size;
      queue_.push_back(std::move(msg));
      pushed_messages_.fetch_add(1, std::memory_order_relaxed);
    }
    cv_.notify_one();
    return true;
  }

  // Drains all queued messages into dest using batch swapping (std::swap).
  // Minimizes mutex lock contention to a single pointer swap.
  bool
  drain(std::deque<TelemetryMessage> &dest,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(50)) {
    dest.clear();
    std::unique_lock<std::mutex> lock(mutex_);
    if (queue_.empty() && !stop_requested_.load(std::memory_order_acquire)) {
      cv_.wait_for(lock, timeout, [this]() {
        return !queue_.empty() ||
               stop_requested_.load(std::memory_order_acquire);
      });
    }

    if (!queue_.empty()) {
      std::swap(queue_, dest);
      current_bytes_ = 0;
      return true;
    }

    return false;
  }

  void requestStop() {
    stop_requested_.store(true, std::memory_order_release);
    cv_.notify_all(); // wake drain() without holding mutex
  }

  bool isStopRequested() const noexcept {
    return stop_requested_.load(std::memory_order_acquire);
  }

  size_t currentBytes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return current_bytes_;
  }

  uint64_t droppedMessages() const noexcept {
    return dropped_messages_.load(std::memory_order_relaxed);
  }

  uint64_t droppedBytes() const noexcept {
    return dropped_bytes_.load(std::memory_order_relaxed);
  }

  uint64_t pushedMessages() const noexcept {
    return pushed_messages_.load(std::memory_order_relaxed);
  }

private:
  const size_t max_bytes_;
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::deque<TelemetryMessage> queue_;
  size_t current_bytes_{0};
  std::atomic<bool> stop_requested_{false};

  std::atomic<uint64_t> dropped_messages_{0};
  std::atomic<uint64_t> dropped_bytes_{0};
  std::atomic<uint64_t> pushed_messages_{0};
};

} // namespace recorder
