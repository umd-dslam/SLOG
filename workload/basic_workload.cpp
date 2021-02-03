#include "workload/basic_workload.h"

#include <fcntl.h>
#include <glog/logging.h>

#include <algorithm>
#include <iomanip>
#include <random>
#include <sstream>
#include <unordered_set>

#include "common/offline_data_reader.h"
#include "common/proto_utils.h"
#include "proto/offline_data.pb.h"

using std::bernoulli_distribution;
using std::iota;
using std::sample;
using std::unordered_set;

namespace slog {
namespace {

// Percentage of multi-home transactions
constexpr char MH_PCT[] = "mh";
// Number of regions selected as homes in a multi-home transaction
constexpr char MH_HOMES[] = "mh_homes";
// Percentage of multi-partition transactions
constexpr char MP_PCT[] = "mp";
// Number of partitions selected as parts of a multi-partition transaction
constexpr char MP_PARTS[] = "mp_parts";
// Number of hot keys per partition. The actual number of
// hot keys won't match exactly the specified number but will be close.
// Precisely, it will be:
//        floor(hot / num_replicas) * num_replicas
constexpr char HOT[] = "hot";
// Number of records in a transaction
constexpr char RECORDS[] = "records";
// Number of hot records in a transaction
constexpr char HOT_RECORDS[] = "hot_records";
// Number of write records in a transaction
constexpr char WRITES[] = "writes";
// Size of a written value in bytes
constexpr char VALUE_SIZE[] = "value_size";
// If set to 1, a SH txn will always be sent to the nearest
// region, a MH txn will always have a part that touches the nearest region
constexpr char NEAREST[] = "nearest";
// Partition that is used in a single-partition transaction.
// Use a negative number to select a random partition for
// each transaction
constexpr char SP_PARTITION[] = "sp_partition";

const RawParamMap DEFAULT_PARAMS = {{MH_PCT, "0"},       {MH_HOMES, "2"}, {MP_PCT, "0"},       {MP_PARTS, "2"},
                                    {HOT, "1000"},       {RECORDS, "10"}, {HOT_RECORDS, "2"},  {WRITES, "2"},
                                    {VALUE_SIZE, "100"}, {NEAREST, "0"},  {SP_PARTITION, "-1"}};

}  // namespace

BasicWorkload::BasicWorkload(const ConfigurationPtr config, const string& data_dir, const string& params_str,
                             const uint32_t seed, const RawParamMap extra_default_params)
    : Workload(MergeParams(extra_default_params, DEFAULT_PARAMS), params_str),
      config_(config),
      partition_to_key_lists_(config->num_partitions()),
      rg_(seed),
      client_txn_id_counter_(0) {
  auto num_replicas = config->num_replicas();
  auto num_partitions = config->num_partitions();
  auto hot_keys_per_list = std::max(1U, params_.GetUInt32(HOT) / num_replicas);
  auto simple_partitioning = config->simple_partitioning();
  for (uint32_t part = 0; part < num_partitions; part++) {
    for (uint32_t rep = 0; rep < num_replicas; rep++) {
      // Initialize hot keys limit for each key list. When keys are added to a list,
      // the first keys are considered hot keys until this limit is reached and any new
      // keys from there are cold keys.
      if (simple_partitioning) {
        partition_to_key_lists_[part].emplace_back(config, part, rep, hot_keys_per_list);
      } else {
        partition_to_key_lists_[part].emplace_back(hot_keys_per_list);
      }
    }
  }

  if (!simple_partitioning) {
    // Load and index the initial data from file if simple partitioning is not used
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
        CHECK_LT(datum.master(), num_replicas) << "Master number exceeds number of replicas";

        partition_to_key_lists_[partition][datum.master()].AddKey(datum.key());
      }
      close(fd);
    }
  }
}

std::pair<Transaction*, TransactionProfile> BasicWorkload::NextTransaction() {
  TransactionProfile pro;

  pro.client_txn_id = client_txn_id_counter_;

  // Decide if this is a multi-partition txn or not
  auto num_partitions = config_->num_partitions();
  auto multi_partition_pct = params_.GetDouble(MP_PCT);
  bernoulli_distribution is_mp(multi_partition_pct / 100);
  pro.is_multi_partition = is_mp(rg_);

  // Select a number of partitions to choose from for each record
  vector<uint32_t> candidate_partitions;
  if (pro.is_multi_partition) {
    candidate_partitions.resize(num_partitions);
    iota(candidate_partitions.begin(), candidate_partitions.end(), 0);
    shuffle(candidate_partitions.begin(), candidate_partitions.end(), rg_);
    auto mp_num_partitions = params_.GetUInt32(MP_PARTS);
    candidate_partitions.resize(mp_num_partitions);
  } else {
    auto sp_partition = params_.GetInt(SP_PARTITION);
    if (sp_partition < 0) {
      std::uniform_int_distribution<uint32_t> dis(0, num_partitions - 1);
      candidate_partitions.push_back(dis(rg_));
    } else {
      CHECK_LT(static_cast<uint32_t>(sp_partition), num_partitions)
          << "Selected single-partition partition does not exist";
      candidate_partitions.push_back(sp_partition);
    }
  }

  // Decide if this is a multi-home txn or not
  auto num_replicas = config_->num_replicas();
  auto multi_home_pct = params_.GetDouble(MH_PCT);
  bernoulli_distribution is_mh(multi_home_pct / 100);
  pro.is_multi_home = is_mh(rg_);

  // Select a number of homes to choose from for each record
  vector<uint32_t> candidate_homes;
  if (pro.is_multi_home) {
    CHECK_GE(num_replicas, 2) << "There must be at least 2 regions for MH txns";
    candidate_homes.resize(num_replicas);
    iota(candidate_homes.begin(), candidate_homes.end(), 0);
    if (params_.GetInt(NEAREST)) {
      std::swap(candidate_homes[0], candidate_homes[config_->local_replica()]);
      shuffle(candidate_homes.begin() + 1, candidate_homes.end(), rg_);
    } else {
      shuffle(candidate_homes.begin(), candidate_homes.end(), rg_);
    }

    auto mp_num_homes = params_.GetUInt32(MH_HOMES);
    CHECK_GE(mp_num_homes, 2) << "At least 2 regions must be selected for MH txns";
    candidate_homes.resize(mp_num_homes);
  } else {
    if (params_.GetInt(NEAREST)) {
      candidate_homes.push_back(config_->local_replica());
    } else {
      std::uniform_int_distribution<uint32_t> dis(0, num_replicas - 1);
      candidate_homes.push_back(dis(rg_));
    }
  }

  vector<KeyEntry> keys;
  std::ostringstream code;

  auto writes = params_.GetUInt32(WRITES);
  auto hot_records = params_.GetUInt32(HOT_RECORDS);
  auto records = params_.GetUInt32(RECORDS);
  auto value_size = params_.GetUInt32(VALUE_SIZE);

  CHECK_LE(writes, records) << "Number of writes cannot exceed number of records in a transaction!";
  CHECK_LE(hot_records, records) << "Number of hot records cannot exceed number of records in a transaction!";

  // Randomly pick some records to be hot records (can be either read or write records)
  for (size_t i = 0; i < records; i++) {
    auto partition = candidate_partitions[i % candidate_partitions.size()];
    auto home = candidate_homes[i % candidate_homes.size()];

    Key key;
    auto is_hot = i < hot_records;
    if (is_hot) {
      key = partition_to_key_lists_[partition][home].GetRandomHotKey(rg_);
    } else {
      key = partition_to_key_lists_[partition][home].GetRandomColdKey(rg_);
    }

    auto ins = pro.records.try_emplace(key, TransactionProfile::Record());
    if (ins.second) {
      auto& record = ins.first->second;
      record.is_hot = is_hot;
      // Decide whether this is a read or a write record
      if (i < writes) {
        code << "SET " << key << " " << RandomString(value_size, rg_) << " ";
        keys.emplace_back(key, KeyType::WRITE);
        record.is_write = true;
      } else {
        code << "GET " << key << " ";
        keys.emplace_back(key, KeyType::READ);
        record.is_write = false;
      }
      record.home = home;
      record.partition = partition;
    }
  }

  // Construct a new transaction
  auto txn = MakeTransaction(keys, code.str());
  txn->mutable_internal()->set_id(client_txn_id_counter_);

  client_txn_id_counter_++;

  return {txn, pro};
}

}  // namespace slog