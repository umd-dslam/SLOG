#pragma once

#include <vector>

#include "workload/basic_workload.h"

namespace slog {

class RemasteringWorkload : public BasicWorkload {
public:
  RemasteringWorkload(
      ConfigurationPtr config,
      const std::string& data_dir,
      const std::string& params_str);

  std::pair<Transaction*, TransactionProfile> NextTransaction();
  std::pair<Transaction*, TransactionProfile> NextRemasterTransaction();

protected:
  static const RawParamMap GetDefaultParams();
};

} // namespace slog