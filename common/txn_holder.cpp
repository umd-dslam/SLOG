#include "common/txn_holder.h"

#include <glog/logging.h>

#include <algorithm>

using std::make_pair;
using std::move;
using std::pair;
using std::string;
using std::unordered_set;
using std::vector;

namespace slog {

using internal::Request;

TxnHolder::TxnHolder(const ConfigurationPtr& config, Transaction* txn) : txn_(txn) {
  keys_in_partition_.clear();
  active_partitions_.clear();

  for (const auto& kv : txn_->read_set()) {
    // If this key is also in write_set, give it write lock instead
    if (config->key_is_in_local_partition(kv.first) && !txn_->write_set().contains(kv.first)) {
      keys_in_partition_.emplace_back(kv.first, LockMode::READ);
    }
  }
  for (const auto& kv : txn_->write_set()) {
    active_partitions_.push_back(config->partition_of_key(kv.first));
    if (config->key_is_in_local_partition(kv.first)) {
      keys_in_partition_.emplace_back(kv.first, LockMode::WRITE);
    }
  }

  std::sort(active_partitions_.begin(), active_partitions_.end());
  auto last = std::unique(active_partitions_.begin(), active_partitions_.end());
  active_partitions_.erase(last, active_partitions_.end());
}

const vector<pair<Key, LockMode>>& TxnHolder::keys_in_partition() const { return keys_in_partition_; }

const std::vector<uint32_t>& TxnHolder::active_partitions() const { return active_partitions_; }

uint32_t TxnHolder::replica_id() const { return replica_id(transaction()); }

uint32_t TxnHolder::replica_id(const Transaction* txn) {
  // This should only be empty for testing.
  // TODO: add metadata to test cases, make this an error
  //
  // Note that this uses all metadata, not just keys in partition. It's therefore safe
  // to call this on transactions that don't involve the current partition
  if (txn->internal().master_metadata().empty()) {
    LOG(WARNING) << "Master metadata empty: txn id " << txn->internal().id();
    return 0;
  }

#ifdef REMASTER_PROTOCOL_COUNTERLESS
  if (txn->internal().type() == TransactionType::LOCK_ONLY && txn->procedure_case() == Transaction::kRemaster &&
      txn->remaster().is_new_master_lock_only()) {
    return txn->remaster().new_master();
  }
#endif

  // Get the master of an element from the metadata. For single-home and lock-onlies, all masters
  // will be the same in the metadata
  return txn->internal().master_metadata().begin()->second.master();
}

}  // namespace slog