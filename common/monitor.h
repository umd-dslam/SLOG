#include <glog/logging.h>

#include "common/types.h"

namespace slog {

inline void MonitorThroughput() {
  using Clock = std::chrono::steady_clock;
  static int64_t counter = 0;
  static int64_t last_counter = 0;
  static Clock::time_point last_time;
  counter++;
  auto span = Clock::now() - last_time;
  if (span > 1s) {
    LOG(INFO) << "Throughput: " << (counter - last_counter) / duration_cast<seconds>(span).count() << " txn/s";

    last_counter = counter;
    last_time = Clock::now();
  }
}

}  // namespace slog