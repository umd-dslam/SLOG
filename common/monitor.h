#include <glog/logging.h>

#include "common/types.h"

namespace slog {

inline void MonitorThroughput() {
  static int64_t counter = 0;
  static int64_t last_counter = 0;
  static TimePoint last_time;
  counter++;
  auto span = Clock::now() - last_time;
  if (span > 1s) {
    LOG(INFO) << "Throughput: " << (counter - last_counter) / duration_cast<seconds>(span).count() << " txn/s";

    last_counter = counter;
    last_time = Clock::now();
  }
}

}  // namespace slog