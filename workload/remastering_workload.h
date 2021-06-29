#pragma once

#include <vector>

#include "workload/basic_workload.h"

namespace slog {

class RemasteringWorkload : public BasicWorkload {
 public:
  RemasteringWorkload(const ConfigurationPtr& config, uint32_t region, const std::string& data_dir,
                      const std::string& params_str, const uint32_t seed = std::random_device()());

  std::pair<Transaction*, TransactionProfile> NextTransaction();
  std::pair<Transaction*, TransactionProfile> NextRemasterTransaction();
};

}  // namespace slog