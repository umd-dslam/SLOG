#include "workload/basic_workload.h"

#include <algorithm>
#include <iomanip>
#include <fcntl.h>
#include <random>
#include <sstream>
#include <unordered_set>
#include <vector>

#include <glog/logging.h>

#include "common/offline_data_reader.h"
#include "common/proto_utils.h"
#include "proto/offline_data.pb.h"

using std::discrete_distribution;
using std::shuffle;
using std::uniform_int_distribution;
using std::string;
using std::vector;
using std::unordered_set;

namespace slog {
namespace {

constexpr char MH_PCT[] = "mh";
constexpr char MH_NUM_HOMES[] = "mh_homes";
constexpr char MP_PCT[] = "mp";
constexpr char MP_NUM_PARTS[] = "mp_parts";
constexpr char NUM_RECORDS[] = "num_records";
constexpr char NUM_WRITES[] = "num_writes";
constexpr char VALUE_SIZE[] = "value_size";

const RawParamMap DEFAULT_PARAMS = {
  { MH_PCT, "0" },
  { MH_NUM_HOMES, "2" },
  { MP_PCT, "0" },
  { MP_NUM_PARTS, "2" },
  { NUM_RECORDS, "10" },
  { NUM_WRITES, "2" },
  { VALUE_SIZE, "100" } // bytes
};

} // namespace

KeyList::KeyList(size_t num_hot_keys) : num_hot_keys_(num_hot_keys) {}

void KeyList::AddKey(Key key) {
  if (hot_keys_.size() < num_hot_keys_) {
    hot_keys_.push_back(key);
    return;
  }
  cold_keys_.push_back(key);
}

Key KeyList::GetRandomHotKey() {
  if (hot_keys_.empty()) {
    throw std::runtime_error("There is no hot key to pick from. Please check your data");
  }
  return PickOne(hot_keys_, re_);
}

Key KeyList::GetRandomColdKey() {
  if (cold_keys_.empty()) {
    throw std::runtime_error("There is no cold key to pick from. Please check your data");
  }
  return PickOne(cold_keys_, re_);
}

BasicWorkload::BasicWorkload(
    const ConfigurationPtr& config,
    const string& data_dir,
    const string& params_str)
  : WorkloadGenerator(DEFAULT_PARAMS, params_str),
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
  auto candidate_homes = Choose(
      config_->GetNumReplicas(),
      pro.is_multi_home ? params_.GetUInt32(MH_NUM_HOMES) : 1,
      re_);

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