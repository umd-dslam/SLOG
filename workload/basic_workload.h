#pragma once

#include <vector>

#include "workload/workload_generator.h"
#include "common/configuration.h"
#include "common/types.h"
#include "proto/transaction.pb.h"

using std::vector;

namespace slog {

namespace {
constexpr char MH_PCT[] = "mh";
constexpr char MH_NUM_HOMES[] = "mh_homes";
constexpr char MP_PCT[] = "mp";
constexpr char MP_NUM_PARTS[] = "mp_parts";
constexpr char NUM_RECORDS[] = "num_records";
constexpr char NUM_WRITES[] = "num_writes";
constexpr char VALUE_SIZE[] = "value_size";

const RawParamMap BASIC_DEFAULT_PARAMS = {
  { MH_PCT, "0" },
  { MH_NUM_HOMES, "2" },
  { MP_PCT, "0" },
  { MP_NUM_PARTS, "2" },
  { NUM_RECORDS, "10" },
  { NUM_WRITES, "2" },
  { VALUE_SIZE, "100" } // bytes
};
} // namespace

class BasicWorkload : public WorkloadGenerator {
public:
  BasicWorkload(
      ConfigurationPtr config,
      const std::string& data_dir,
      const std::string& params_str,
      const RawParamMap extra_default_params = {});

  std::pair<Transaction*, TransactionProfile> NextTransaction();

protected:
  ConfigurationPtr config_;

  // This is an index of keys by their partition and home.
  // Each partition holds a vector of homes, each of which
  // is a list of keys.
  vector<vector<KeyList>> partition_to_key_lists_;

  std::mt19937 re_;

  TxnId client_txn_id_counter_;

  static const RawParamMap GetDefaultParams();
};

} // namespace slog