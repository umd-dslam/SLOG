#pragma once

#include <glog/logging.h>

#include <chrono>
#include <thread>

namespace slog {

inline void SetThreadName(pthread_t thread, const char* name) { pthread_setname_np(thread, name); }

inline void PinToCpu(pthread_t thread, int cpu) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu, &cpuset);
  int rc = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
  if (rc != 0) {
    LOG(ERROR) << "Failed to pin thread to CPU " << cpu << ". Error code: " << rc;
  }
}

}  // namespace slog