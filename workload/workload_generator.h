#pragma once

#include "proto/transaction.pb.h"

namespace slog {

/**
 * Base class for a workload generator
 */
class WorkloadGenerator {
public:
  /**
   * Gets the next transaction in the workload
   */
  virtual Transaction NextTransaction() = 0;
};

} // namespace slog