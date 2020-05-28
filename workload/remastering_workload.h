#pragma once

#include <vector>

#include "workload/basic_workload.h"

namespace slog {

namespace {
constexpr char REMASTER_GAP[] = "remaster_gap";

const RawParamMap REMASTERING_DEFAULT_PARAMS = {
  { REMASTER_GAP, "50" }
};
} // namespace

class RemasteringWorkload : public BasicWorkload {
public:
  RemasteringWorkload(
      ConfigurationPtr config,
      const std::string& data_dir,
      const std::string& params_str,
      const RawParamMap extra_default_params = {});

  std::pair<Transaction*, TransactionProfile> NextTransaction();
  std::pair<Transaction*, TransactionProfile> NextRemasterTransaction();

protected:
  static const RawParamMap GetDefaultParams();
};

} // namespace slog