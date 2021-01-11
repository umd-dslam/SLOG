#include "common/monitor.h"

#include <glog/logging.h>

namespace slog {

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