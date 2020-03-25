#pragma once

#include "proto/transaction.pb.h"

namespace slog {

const uint32_t MP_NUM_PARTITIONS = 2;
const uint32_t MH_NUM_HOMES = 2;
const uint32_t NUM_RECORDS = 10;
const uint32_t NUM_WRITES = 2;
const uint32_t VALUE_SIZE = 100; // bytes

/**
 * Base class for a workload generator
 */
class WorkloadGenerator {
public:
  /**
   * Gets the next transaction in the workload
   */
  virtual Transaction* NextTransaction() = 0;
};

} // namespace slog