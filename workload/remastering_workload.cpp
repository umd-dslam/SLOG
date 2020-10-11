#include "workload/remastering_workload.h"
#include "common/proto_utils.h"

using std::discrete_distribution;
using std::unordered_set;

namespace slog {
namespace {

// Number of normal transactions to send between each remastering
constexpr char REMASTER_GAP[] = "remaster_gap";

const RawParamMap DEFAULT_PARAMS = {
  { REMASTER_GAP, "50" }
};

} // namespace

RemasteringWorkload::RemasteringWorkload(
    const ConfigurationPtr config,
    const string& data_dir,
    const string& params_str)
  : BasicWorkload(
      config,
      data_dir,
      params_str,
      DEFAULT_PARAMS) {}

std::pair<Transaction*, TransactionProfile>
RemasteringWorkload::NextTransaction() {
  if (client_txn_id_counter_ % params_.GetUInt32(REMASTER_GAP) == 0) {
    return NextRemasterTransaction();
  } else {
    return BasicWorkload::NextTransaction();
  }
}

std::pair<Transaction*, TransactionProfile>
RemasteringWorkload::NextRemasterTransaction() {

  TransactionProfile pro;

  pro.client_txn_id = client_txn_id_counter_;

  pro.is_multi_home = false;
  pro.is_multi_partition = false;

  unordered_set<Key> read_set;
  unordered_set<Key> write_set;
  unordered_map<Key, pair<uint32_t, uint32_t>> metadata;

  auto home = Choose(config_->num_replicas(), 1, rg_)[0];
  auto partition = Choose(config_->num_partitions(), 1, rg_)[0];

  auto new_master = (home + 1) % config_->num_replicas();

  auto key = partition_to_key_lists_[partition][home].GetRandomColdKey();
  write_set.insert(key);

  pro.key_to_home[key] = home;
  pro.key_to_partition[key] = partition;

  auto txn = MakeTransaction(
    read_set,
    write_set,
    "",
    metadata,
    MakeMachineId("0:0"),
    new_master);
  txn->mutable_internal()->set_id(client_txn_id_counter_);

  client_txn_id_counter_++;

  return std::make_pair(txn, pro);
}

};