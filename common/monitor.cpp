#include "common/monitor.h"

#include <glog/logging.h>

namespace slog {

void MonitorThroughput() {
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

uint32_t gLocalMachineId = 0;
uint64_t gDisabledTracingEvents = 0;

void InitializeTracing(const ConfigurationPtr& config) {
  gLocalMachineId = config->local_machine_id();
  auto events = config->disabled_tracing_events();
  for (auto e : events) {
    gDisabledTracingEvents |= (1 << e);
  }
}

}  // namespace slog