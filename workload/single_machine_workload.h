#pragma once

#include <vector>

#include "workload/workload_generator.h"
#include "common/configuration.h"
#include "common/types.h"
#include "proto/transaction.pb.h"

using std::vector;

namespace slog {

class SingleMachineWorkload : public WorkloadGenerator {
public:
  SingleMachineWorkload(
      ConfigurationPtr config,
      const std::string& data_dir,
      const std::string& params_str);

  std::pair<Transaction*, TransactionProfile> NextTransaction() final;

private:
  ConfigurationPtr config_;

  vector<Key> keys_;

  std::mt19937 re_;

  TxnId client_txn_id_counter_;
};

} // namespace slog