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
// Number of hot keys across the key space. The actual number of
// hot keys won't match exactly the specified number but will be close. 
// Precisely, it will be:
//        floor(hot / num_key_lists) * num_key_lists
//  where: num_key_lists = total_num_replicas * total_num_partitions
constexpr char HOT[] = "hot";
// Number of records in a transaction
constexpr char NUM_RECORDS[] = "records";
// Number of hot records in a transaction
constexpr char NUM_HOT_RECORDS[] = "hot_records";
// Number of write records in a transaction
constexpr char NUM_WRITES[] = "writes";
// Size of a written value in bytes
constexpr char VALUE_SIZE[] = "value_size";
// Region that is home to a single-home transaction.
// Use a negative number to select a random region for
// each transaction
constexpr char SH_REGION[] = "sh_region";
// Partition that is used in a single-partition transaction.
// Use a negative number to select a random partition for
// each transaction
constexpr char SP_PARTITION[] = "sp_partition";

const RawParamMap DEFAULT_PARAMS = {
  { MH_PCT, "0" },
  { MH_NUM_HOMES, "2" },
  { MP_PCT, "0" },
  { MP_NUM_PARTS, "2" },
  { HOT, "10000" },
  { NUM_RECORDS, "10" },
  { NUM_HOT_RECORDS, "2" },
  { NUM_WRITES, "4" },
  { VALUE_SIZE, "100" },
  { SH_REGION, "-1" },
  { SP_PARTITION, "-1" }
};

} // namespace

BasicWorkload::BasicWorkload(
    const ConfigurationPtr config,
    const string& data_dir,
    const string& params_str,
    const RawParamMap extra_default_params)
  : WorkloadGenerator(
        MergeParams(extra_default_params, DEFAULT_PARAMS),
        params_str),
    config_(config),
    partition_to_key_lists_(config->GetNumPartitions()),
    client_txn_id_counter_(0) {
  auto num_replicas = config->GetNumReplicas();
  auto num_partitions = config->GetNumPartitions();
  auto hot_keys_per_list = params_.GetUInt32(HOT) / (num_replicas * num_partitions);
  for (auto& key_lists : partition_to_key_lists_) {
    for (uint32_t rep = 0; rep < num_replicas; rep++) {
      // Initialize hot keys limit for each key list. When keys are added to a list, 
      // the first keys are considered hot keys until this limit is reached and any new
      // keys from there are cold keys.
      key_lists.emplace_back(hot_keys_per_list);
    }
  }

  // Load and index the initial data
  for (uint32_t partition = 0; partition < num_partitions; partition++) {
    auto data_file = data_dir + "/" + std::to_string(partition) + ".dat";
    auto fd = open(data_file.c_str(), O_RDONLY);
    if (fd < 0) {
      LOG(FATAL) << "Error while loading \"" << data_file << "\": " << strerror(errno);
    }

    OfflineDataReader reader(fd);
    LOG(INFO) << "Loading " << reader.GetNumDatums() << " datums from " << data_file;
    while (reader.HasNextDatum()) {
      auto datum = reader.GetNextDatum();
      CHECK_LT(datum.master(), num_replicas)
          << "Master number exceeds number of replicas";

      partition_to_key_lists_[partition][datum.master()].AddKey(datum.key());
    }
    close(fd);
  }
}

std::pair<Transaction*, TransactionProfile>
BasicWorkload::NextTransaction() {
  TransactionProfile pro;

  pro.client_txn_id = client_txn_id_counter_;

  // Decide if this is a multi-partition txn or not
  auto num_partitions = config_->GetNumPartitions();
  auto multi_partition_pct = params_.GetDouble(MP_PCT);
  discrete_distribution<> spmp({100 - multi_partition_pct, multi_partition_pct});
  pro.is_multi_partition = spmp(re_);

  // Select a number of partitions to choose from for each record
  vector<uint32_t> candidate_partitions;
  if (pro.is_multi_partition) {
    auto mp_num_partitions = params_.GetUInt32(MP_NUM_PARTS);
    candidate_partitions = Choose(num_partitions, mp_num_partitions, re_);
  } else {
    auto sp_partition = params_.GetInt(SP_PARTITION);
    if (sp_partition < 0) {
      candidate_partitions = Choose(num_partitions, 1, re_);
    } else {
      CHECK_LT(static_cast<uint32_t>(sp_partition), num_partitions)
          << "Selected single-partition partition does not exist";
      candidate_partitions.push_back(sp_partition);
    }
  }

  // Decide if this is a multi-home txn or not
  auto num_replicas = config_->GetNumReplicas();
  auto multi_home_pct = params_.GetDouble(MH_PCT);
  discrete_distribution<> shmh({100 - multi_home_pct, multi_home_pct});
  pro.is_multi_home = shmh(re_);

  // Select a number of homes to choose from for each record
  vector<uint32_t> candidate_homes;
  if (pro.is_multi_home) {
    auto mh_num_homes = params_.GetUInt32(MH_NUM_HOMES);
    candidate_homes = Choose(num_replicas, mh_num_homes, re_);
  } else {
    auto sh_region = params_.GetInt(SH_REGION);
    if (sh_region < 0) {
      candidate_homes = Choose(num_replicas, 1, re_);
    } else {
      CHECK_LT(static_cast<uint32_t>(sh_region), num_replicas)
          << "Selected single-home region does not exist";
      candidate_homes.push_back(sh_region);
    }
  }

  unordered_set<Key> read_set;
  unordered_set<Key> write_set;
  std::ostringstream code;

  auto num_writes = params_.GetUInt32(NUM_WRITES);
  auto num_hot_records = params_.GetUInt32(NUM_HOT_RECORDS);
  auto num_records = params_.GetUInt32(NUM_RECORDS);
  auto value_size = params_.GetUInt32(VALUE_SIZE);

  CHECK_LE(num_writes, num_records)
      << "Number of writes cannot exceed number of records in a transaction!";
  CHECK_LE(num_hot_records, num_records)
      << "Number of hot records cannot exceed number of records in a transaction!";

  // Randomly pick some records to be hot records (can be either read or write records)
  auto hot_indices = Choose(num_records, num_hot_records, re_);
  for (size_t i = 0; i < num_records; i++) {
    auto partition = candidate_partitions[i % candidate_partitions.size()];
    auto home = candidate_homes[i % candidate_homes.size()];
    
    Key key;
    // Decide whether to pick a hot or cold key
    if (std::find(hot_indices.begin(), hot_indices.end(), i) != hot_indices.end()) {
      key = partition_to_key_lists_[partition][home].GetRandomHotKey();
      pro.is_hot_record[key] = true;
    } else {
      key = partition_to_key_lists_[partition][home].GetRandomColdKey();
      pro.is_hot_record[key] = false;
    }

    // Decide whether this is a read or a write record
    if (i < num_writes) {
      code << "SET " << key << " " << RandomString(value_size, re_) << " ";
      write_set.insert(key);
      pro.is_write_record[key] = true;
    } else {
      code << "GET " << key << " ";
      read_set.insert(key);
      pro.is_write_record[key] = false;
    }

    pro.key_to_home[key] = home;
    pro.key_to_partition[key] = partition;
  }

  // Construct a new transaction
  auto txn = MakeTransaction(read_set, write_set, code.str());
  txn->mutable_internal()->set_id(client_txn_id_counter_);

  client_txn_id_counter_++;

  return {txn, pro};
}

} // namespace slog