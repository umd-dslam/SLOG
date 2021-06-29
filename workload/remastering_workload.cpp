#include "workload/remastering_workload.h"

#include "common/proto_utils.h"

using std::uniform_int_distribution;
using std::unordered_set;

namespace slog {
namespace {

// Number of normal transactions to send between each remastering
constexpr char REMASTER_GAP[] = "remaster_gap";

const RawParamMap DEFAULT_PARAMS = {{REMASTER_GAP, "50"}};

}  // namespace

RemasteringWorkload::RemasteringWorkload(const ConfigurationPtr& config, uint32_t region, const string& data_dir,
                                         const string& params_str, const uint32_t seed)
    : BasicWorkload(config, region, data_dir, params_str, seed, DEFAULT_PARAMS) {
  name_ = "remastering";
}

std::pair<Transaction*, TransactionProfile> RemasteringWorkload::NextTransaction() {
  if (client_txn_id_counter_ % params_.GetUInt32(REMASTER_GAP) == 0) {
    return NextRemasterTransaction();
  } else {
    return BasicWorkload::NextTransaction();
  }
}

std::pair<Transaction*, TransactionProfile> RemasteringWorkload::NextRemasterTransaction() {
  TransactionProfile pro;

  pro.client_txn_id = client_txn_id_counter_;

  pro.is_multi_home = false;
  pro.is_multi_partition = false;

  vector<KeyMetadata> keys;

  auto home = uniform_int_distribution<>(0, config_->num_replicas() - 1)(rg_);
  auto partition = uniform_int_distribution<>(0, config_->num_partitions() - 1)(rg_);

  auto new_master = (home + 1) % config_->num_replicas();

  auto key = partition_to_key_lists_[partition][home].GetRandomColdKey(rg_);
  keys.emplace_back(key, KeyType::WRITE);
  TransactionProfile::Record record{
      .partition = static_cast<uint32_t>(partition),
      .home = static_cast<uint32_t>(home),
      .is_hot = false,
      .is_write = true,
  };

  pro.records.insert({key, record});

  auto txn = MakeTransaction(keys, {}, new_master);
  txn->mutable_internal()->set_id(client_txn_id_counter_);

  client_txn_id_counter_++;

  return std::make_pair(txn, pro);
}

};  // namespace slog