#pragma once

#include <thread>

#include <glog/logging.h>

namespace slog {

inline void PinToCpu(pthread_t thread, int cpu) {
  if (cpu >= 0) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu, &cpuset);
    int rc = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      LOG(ERROR) << "Failed to pin thread to CPU " << cpu << ". Error code: " << rc;
    }
  }
}

} // namespace slog