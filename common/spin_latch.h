#pragma once

#include <atomic>

// See https://rigtorp.se/spinlock/
class SpinLatch {
 public:
  void lock() {
    for (;;) {
      if (!lock_.exchange(true)) {
        break;
      }
      while (lock_.load())
        ;
    };
  }

  void unlock() { lock_.store(false); }

 private:
  std::atomic<bool> lock_ = {false};
};
