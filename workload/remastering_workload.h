#pragma once

#include <vector>

#include "workload/workload_generator.h"
#include "common/configuration.h"
#include "common/types.h"
#include "proto/transaction.pb.h"

using std::vector;

namespace slog {

class RemasteringWorkload : public WorkloadGenerator {
public:
  RemasteringWorkload(
      ConfigurationPtr config,
      const std::string& data_dir,
      const std::string& params_str);

  std::pair<Transaction*, TransactionProfile> NextTransaction() final;

private:
  Transaction* MakeNormalTransaction(TransactionProfile& pro);
  Transaction* MakeRemasterTransaction(TransactionProfile& pro);


  ConfigurationPtr config_;

  // This is an index of keys by their partition and home.
  // Each partition holds a vector of homes, each of which
  // is a list of keys.
  vector<vector<KeyList>> partition_to_key_lists_;

  std::mt19937 re_;

  TxnId client_txn_id_counter_;
};

} // namespace slog