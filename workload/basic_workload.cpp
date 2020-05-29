#include "workload/basic_workload.h"

#include <algorithm>
#include <iomanip>
#include <fcntl.h>
#include <random>
#include <sstream>
#include <unordered_set>

#include <glog/logging.h>

#include "common/offline_data_reader.h"
#include "common/proto_utils.h"
#include "proto/offline_data.pb.h"

using std::discrete_distribution;
using std::unordered_set;

namespace slog {

namespace {
// Percentage of multi-home transactions
constexpr char MH_PCT[] = "mh";
// Number of regions selected as homes in a multi-home transaction
constexpr char MH_NUM_HOMES[] = "mh_homes";
// Percentage of multi-partition transactions
constexpr char MP_PCT[] = "mp";
// Number of partitions selected as parts of a multi-partition transaction
constexpr char MP_NUM_PARTS[] = "mp_parts";
// Number of records in a transaction
constexpr char NUM_RECORDS[] = "num_records";
// Number of write records in a transaction
constexpr char NUM_WRITES[] = "num_writes";
// Size of a written value in bytes
constexpr char VALUE_SIZE[] = "value_size";
// Region that is home to a single-home transaction.
// Use a negative number to select a random region for
// each transaction
constexpr char SH_REGION[] = "sh_region";

const RawParamMap DEFAULT_PARAMS = {
  { MH_PCT, "0" },
  { MH_NUM_HOMES, "2" },
  { MP_PCT, "0" },
  { MP_NUM_PARTS, "2" },
  { NUM_RECORDS, "10" },
  { NUM_WRITES, "2" },
  { VALUE_SIZE, "100" },
  { SH_REGION, "-1" }
};
} // namespace

BasicWorkload::BasicWorkload(
    ConfigurationPtr config,
    const string& data_dir,
    const string& params_str,
    const RawParamMap extra_default_params)
  : WorkloadGenerator(
        MergeParams(extra_default_params, DEFAULT_PARAMS),
        params_str),
    config_(config),
    partition_to_key_lists_(config->GetNumPartitions()),
    client_txn_id_counter_(0) {

  for (auto& key_lists : partition_to_key_lists_) {
    for (uint32_t rep = 0; rep < config->GetNumReplicas(); rep++) {
      // TODO (ctring): initialize each key list with some hot keys
      key_lists.emplace_back();
    }
  }

  // Load and index the initial data
  for (uint32_t partition = 0; partition < config->GetNumPartitions(); partition++) {
    auto data_file = data_dir + "/" + std::to_string(partition) + ".dat";
    auto fd = open(data_file.c_str(), O_RDONLY);
    if (fd < 0) {
      LOG(FATAL) << "Error while loading \"" << data_file << "\": " << strerror(errno);
    }

    OfflineDataReader reader(fd);
    LOG(INFO) << "Loading " << reader.GetNumDatums() << " datums from " << data_file;
    while (reader.HasNextDatum()) {
      auto datum = reader.GetNextDatum();
      CHECK_LT(datum.master(), config->GetNumReplicas())
          << "Master number exceeds number of replicas";

      partition_to_key_lists_[partition][datum.master()].AddKey(datum.key());
    }
    close(fd);
  }
}

std::pair<Transaction*, TransactionProfile>
BasicWorkload::NextTransaction() {
  auto num_writes = params_.GetUInt32(NUM_WRITES);
  auto num_records = params_.GetUInt32(NUM_RECORDS);
  CHECK_LE(num_writes, num_records)
      << "Number of writes cannot exceed number of records in a txn!";

  TransactionProfile pro;

  pro.client_txn_id = client_txn_id_counter_;

  // Decide if this is a multi-partition txn or not
  auto multi_partition_pct = params_.GetDouble(MP_PCT);
  discrete_distribution<> spmp({100 - multi_partition_pct, multi_partition_pct});
  pro.is_multi_partition = spmp(re_);

  // Select a number of partitions to choose from for each record
  auto candidate_partitions = Choose(
      config_->GetNumPartitions(),
      pro.is_multi_partition ? params_.GetUInt32(MP_NUM_PARTS) : 1,
      re_);

  // Decide if this is a multi-home txn or not
  auto multi_home_pct = params_.GetDouble(MH_PCT);
  discrete_distribution<> shmh({100 - multi_home_pct, multi_home_pct});
  pro.is_multi_home = shmh(re_);

  // Select a number of homes to choose from for each record
  vector<uint32_t> candidate_homes;
  if (pro.is_multi_home) {
    candidate_homes = Choose(
        config_->GetNumReplicas(),
        params_.GetUInt32(MH_NUM_HOMES),
        re_);
  } else {
    auto sh_region = params_.GetInt(SH_REGION);
    if (sh_region < 0) {
      candidate_homes = Choose(config_->GetNumReplicas(), 1, re_);
    } else {
      CHECK_LT(static_cast<uint32_t>(sh_region), config_->GetNumReplicas())
          << "Selected single-home region does not exist";
      candidate_homes.push_back(sh_region);
    }
  }

  unordered_set<Key> read_set;
  unordered_set<Key> write_set;
  std::ostringstream code;

  // Fill txn with write operators
  auto value_size = params_.GetUInt32(VALUE_SIZE);
  for (size_t i = 0; i < num_writes; i++) {
    auto partition = candidate_partitions[i % candidate_partitions.size()];
    auto home = candidate_homes[i % candidate_homes.size()];
    // TODO: Add hot keys later
    auto key = partition_to_key_lists_[partition][home].GetRandomColdKey();
    code << "SET " << key << " " << RandomString(value_size, re_) << " ";
    write_set.insert(key);

    pro.key_to_home[key] = home;
    pro.key_to_partition[key] = partition;
  }

  // Fill txn with read operators
  for (size_t i = 0; i < num_records - num_writes; i++) {
    auto partition = candidate_partitions[(num_writes + i) % candidate_partitions.size()];
    auto home = candidate_homes[(num_writes + i) % candidate_homes.size()];
    // TODO: Add hot keys later
    auto key = partition_to_key_lists_[partition][home].GetRandomColdKey();
    code << "GET " << key << " ";
    read_set.insert(key);

    pro.key_to_home[key] = home;
    pro.key_to_partition[key] = partition;
  }

  auto txn = MakeTransaction(read_set, write_set, code.str());
  txn->mutable_internal()->set_id(client_txn_id_counter_);

  client_txn_id_counter_++;

  return {txn, pro};
}

} // namespace slog