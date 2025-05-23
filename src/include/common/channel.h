//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// channel.h
//
// Identification: src/include/common/channel.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <condition_variable>  // NOLINT
#include <mutex>               // NOLINT
#include <queue>
#include <utility>

namespace bustub {

/**
 * Channels allow for safe sharing of data between threads. This is a multi-producer multi-consumer channel.
 */
template <class T>
class Channel {
 public:
  Channel() = default;
  ~Channel() = default;

  /**
   * @brief Inserts an element into a shared queue.
   *
   * @param element The element to be inserted.
   */
  void Put(T element) {
    std::unique_lock<std::mutex> lk(m_);
    q_.push(std::move(element));
    lk.unlock();
    cv_.notify_all();
  }

  /**
   * @brief Gets an element from the shared queue. If the queue is empty, blocks until an element is available.
   */
  auto Get() -> T {
    std::unique_lock<std::mutex> lk(m_);
    cv_.wait(lk, [&]() { return !q_.empty(); });
    T element = std::move(q_.front());
    q_.pop();
    return element;
  }

 private:
  std::mutex m_;
  std::condition_variable cv_;
  std::queue<T> q_;
};
}  // namespace bustub

// quick note about std::condition_variable
// 1. a thread has to own a lock to call cv.wait(lock, predicate)
// 2. after calling cv.wait(), the thread is suspent and release the lock (so other thread might wait for the same
// condition as well)
// 3. to call cv.notify() the thread doesnt necessarily need own a lock,
//    however the best practice is the thread get the lock first, update some shared states and release lock and notify
//    this is to avoid data race
// 4. when the waiting thread(s) receive notify they will do following:
//    1) try acquire the lock passed in wait()
//    2) check the if predicate is still fullfill (this is to avoid) that there might be state changes when the lock is
//    unlocked and notify() are sent 3) if the above 2 check is true, the thread is woken up and own the lock again to
//    process
//       otherwise the thread will wait again.