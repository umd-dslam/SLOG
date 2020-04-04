#pragma once

#include <unordered_map>

#include "common/types.h"
#include "proto/transaction.pb.h"

namespace slog {

const uint32_t MP_NUM_PARTITIONS = 2;
const uint32_t MH_NUM_HOMES = 2;
const uint32_t NUM_RECORDS = 10;
const uint32_t NUM_WRITES = 2;
const uint32_t VALUE_SIZE = 100; // bytes

struct TransactionProfile {
  bool is_multi_home;
  bool is_multi_partition;

  std::unordered_map<Key, uint32_t> key_to_partition;
  std::unordered_map<Key, uint32_t> key_to_home;
};

/**
 * Base class for a workload generator
 */
class WorkloadGenerator {
public:
  /**
   * Gets the next transaction in the workload
   */
  virtual std::pair<Transaction*, TransactionProfile> NextTransaction() = 0;
};

} // namespace slog