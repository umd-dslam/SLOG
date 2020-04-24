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
using ReplicaId = uint32_t;
using TxnReplicaId = std::pair<TxnId, ReplicaId>;

struct Metadata {
  Metadata() = default;
  Metadata(ReplicaId m, uint32_t c = 0) : master(m), counter(c) {}
  void operator=(const MasterMetadata& metadata) {
    master = metadata.master();
    counter = metadata.counter();
  }

  uint32_t master;
  uint32_t counter;
};

struct Record {
  Record(Value v, ReplicaId m, uint32_t c = 0) : value(v), metadata(m, c) {}
  Record() = default;

  Value value;
  Metadata metadata;
};

using Clock = steady_clock;
using TimePoint = Clock::time_point;
using Duration = Clock::duration;

enum class LockMode { UNLOCKED, READ, WRITE };

} // namespace slog