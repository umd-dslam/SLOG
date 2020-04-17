#include "workload/single_machine_workload.h"

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

constexpr char REPLICA[] = "rep";
constexpr char PARTITION[] = "part";
constexpr char NUM_RECORDS[] = "num_records";
constexpr char NUM_WRITES[] = "num_writes";
constexpr char VALUE_SIZE[] = "value_size";

const RawParamMap DEFAULT_PARAMS = {
  { REPLICA, "0" },
  { PARTITION, "0" },
  { NUM_RECORDS, "10" },
  { NUM_WRITES, "2" },
  { VALUE_SIZE, "100" } // bytes
};

} // namespace

SingleMachineWorkload::SingleMachineWorkload(
    ConfigurationPtr config,
    const string& data_dir,
    const string& params_str)
  : WorkloadGenerator(DEFAULT_PARAMS, params_str),
    config_(config),
    client_txn_id_counter_(0) {

  auto data_file = data_dir + "/" + params_.GetString(PARTITION) + ".dat";
  auto fd = open(data_file.c_str(), O_RDONLY);
  if (fd < 0) {
    LOG(FATAL) << "Error while loading \"" << data_file << "\": " << strerror(errno);
  }

  OfflineDataReader reader(fd);
  LOG(INFO) << "Loading " << reader.GetNumDatums() << " datums from " << data_file;

  auto replica = params_.GetUInt32(REPLICA);
  while (reader.HasNextDatum()) {
    auto datum = reader.GetNextDatum();
    CHECK_LT(datum.master(), config->GetNumReplicas())
        << "Master number exceeds number of replicas";
    
    if (datum.master() == replica) {
      keys_.push_back(datum.key());
    }
  }
  close(fd);

  if (keys_.empty()) {
    throw std::runtime_error("The list of key is empty");
  }
}

std::pair<Transaction*, TransactionProfile>
SingleMachineWorkload::NextTransaction() {
  auto num_writes = params_.GetUInt32(NUM_WRITES);
  auto num_records = params_.GetUInt32(NUM_RECORDS);
  CHECK_LE(num_writes, num_records) 
      << "Number of writes cannot exceed number of records in a txn!";

  TransactionProfile pro;
  pro.client_txn_id = client_txn_id_counter_;
  pro.is_multi_partition = false;
  pro.is_multi_home = false;

  unordered_set<Key> read_set;
  unordered_set<Key> write_set;
  std::ostringstream code;
  auto replica = params_.GetUInt32(REPLICA);
  auto partition = params_.GetUInt32(PARTITION);

  // Fill txn with write operators
  auto value_size = params_.GetUInt32(VALUE_SIZE);
  for (size_t i = 0; i < num_writes; i++) {
    auto key = PickOne(keys_, re_);
    code << "SET " << key << " " << RandomString(value_size, re_) << " ";
    write_set.insert(key);

    pro.key_to_home[key] = replica;
    pro.key_to_partition[key] = partition;
  }

  // Fill txn with read operators
  for (size_t i = 0; i < num_records - num_writes; i++) {
    auto key = PickOne(keys_, re_);
    code << "GET " << key << " ";
    read_set.insert(key);

    pro.key_to_home[key] = replica;
    pro.key_to_partition[key] = partition;
  }

  auto txn = MakeTransaction(read_set, write_set, code.str());
  txn->mutable_internal()->set_id(client_txn_id_counter_);

  client_txn_id_counter_++;

  return {txn, pro};
}

} // namespace slog