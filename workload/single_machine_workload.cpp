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

SingleMachineWorkload::SingleMachineWorkload(
    ConfigurationPtr config,
    std::string data_dir,
    uint32_t replica,
    uint32_t partition)
  : config_(config),
    replica_(replica),
    partition_(partition),
    client_txn_id_counter_(0) {

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
    
    if (datum.master() == replica) {
      keys_.push_back(datum.key());
    }
  }
  close(fd);
}

std::pair<Transaction*, TransactionProfile>
SingleMachineWorkload::NextTransaction() {
  CHECK_LE(NUM_WRITES, NUM_RECORDS) 
      << "Number of writes cannot exceed number of records in a txn!";

  TransactionProfile pro;

  pro.client_txn_id = client_txn_id_counter_;
  pro.is_multi_partition = false;
  pro.is_multi_home = false;

  unordered_set<Key> read_set;
  unordered_set<Key> write_set;
  std::ostringstream code;

  // Fill txn with write operators
  for (size_t i = 0; i < NUM_WRITES; i++) {
    auto key = PickOne(keys_, re_);
    code << "SET " << key << " " << RandomString(VALUE_SIZE, re_) << " ";
    write_set.insert(key);

    pro.key_to_home[key] = replica_;
    pro.key_to_partition[key] = partition_;
  }

  // Fill txn with read operators
  for (size_t i = 0; i < NUM_RECORDS - NUM_WRITES; i++) {
    auto key = PickOne(keys_, re_);
    code << "GET " << key << " ";
    read_set.insert(key);

    pro.key_to_home[key] = replica_;
    pro.key_to_partition[key] = partition_;
  }

  auto txn = MakeTransaction(read_set, write_set, code.str());
  txn->mutable_internal()->set_id(client_txn_id_counter_);

  client_txn_id_counter_++;

  return {txn, pro};
}

} // namespace slog