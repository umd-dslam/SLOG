#pragma once

#include <chrono>
#include <string>

#include "proto/transaction.pb.h"

using namespace std::chrono;

namespace slog {

using Key = std::string;
using Value = std::string;
using TxnId = uint32_t;
using BatchId = uint32_t;
using SlotId = uint32_t;

struct Metadata {
  Metadata() = default;
  Metadata(uint32_t m, uint32_t c = 0) : master(m), counter(c) {}
  void operator=(const MasterMetadata& metadata) {
    master = metadata.master();
    counter = metadata.counter();
  }

  uint32_t master;
  uint32_t counter;
};

struct Record {
  Record(Value v, uint32_t m, uint32_t c = 0) : value(v), metadata(m, c) {}
  Record() = default;

  Value value;
  Metadata metadata;
};

enum class LockMode { UNLOCKED, READ, WRITE };

using Clock = steady_clock;
using TimePoint = Clock::time_point;
using Duration = Clock::duration;

enum class LockMode { UNLOCKED, READ, WRITE };

} // namespace slog