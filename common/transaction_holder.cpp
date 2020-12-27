#include "common/transaction_holder.h"

#include <algorithm>

#include <glog/logging.h>

using std::pair;
using std::make_pair;
using std::move;
using std::vector;
using std::unordered_set;
using std::string;

namespace slog {

using internal::Request;

TransactionHolder::TransactionHolder() : txn_request_(nullptr) {}
TransactionHolder::TransactionHolder(const ConfigurationPtr& config, ReusableRequest&& req) {
  SetTxnRequest(config, move(req));
}

void TransactionHolder::SetTxnRequest(const ConfigurationPtr& config, ReusableRequest&& req) {
  auto& txn = req.get()->forward_txn().txn();
  keys_in_partition_.clear();
  active_partitions_.clear();
  involved_replicas_.clear();
  vector<uint32_t> involved_partitions;

  // TODO: involved_partitions_ is only needed by MH and SH, could avoid computing for LO
  for (const auto& kv : txn.read_set()) {
    involved_partitions.push_back(config->partition_of_key(kv.first));
    // If this key is also in write_set, give it write lock instead
    if (config->key_is_in_local_partition(kv.first) 
        && !txn.write_set().contains(kv.first)) {
      keys_in_partition_.emplace_back(kv.first, LockMode::READ);
    }
  }
  for (const auto& kv : txn.write_set()) {
    involved_partitions.push_back(config->partition_of_key(kv.first));
    active_partitions_.push_back(config->partition_of_key(kv.first));
    if (config->key_is_in_local_partition(kv.first)) {
      keys_in_partition_.emplace_back(kv.first, LockMode::WRITE);
    }
  }

  // TODO: only needed for MH
  for (auto& pair : txn.internal().master_metadata()) {
    involved_replicas_.push_back(pair.second.master());
  }

  // Deduplicate the involved partitions/replicas arrays

  {
    std::sort(involved_partitions.begin(), involved_partitions.end());
    auto last = std::unique(involved_partitions.begin(), involved_partitions.end());
    num_involved_partitions_ = last - involved_partitions.begin();
  }

  {
    std::sort(active_partitions_.begin(), active_partitions_.end());
    auto last = std::unique(active_partitions_.begin(), active_partitions_.end());
    active_partitions_.erase(last, active_partitions_.end());
  }

  {
    std::sort(involved_replicas_.begin(), involved_replicas_.end());
    auto last = std::unique(involved_replicas_.begin(), involved_replicas_.end());
    involved_replicas_.erase(last, involved_replicas_.end());
  }

  txn_request_ = move(req);
}

const vector<pair<Key, LockMode>>& TransactionHolder::keys_in_partition() const {
  return keys_in_partition_;
}

uint32_t TransactionHolder::num_involved_partitions() const {
  return num_involved_partitions_;
}

const std::vector<uint32_t>& TransactionHolder::active_partitions() const {
  return active_partitions_;
}

const std::vector<uint32_t>& TransactionHolder::involved_replicas() const {
  return involved_replicas_;
}

vector<ReusableRequest>& TransactionHolder::early_remote_reads() {
  return early_remote_reads_;
}

uint32_t TransactionHolder::replica_id() const {
  return replica_id(transaction());
}

uint32_t TransactionHolder::replica_id(const Transaction* txn) {
  // This should only be empty for testing.
  // TODO: add metadata to test cases, make this an error
  //
  // Note that this uses all metadata, not just keys in partition. It's therefore safe
  // to call this on transactions that don't involve the current partition
  if (txn->internal().master_metadata().empty()) {
    LOG(WARNING) << "Master metadata empty: txn id " << txn->internal().id();
    return 0;
  }
  // Get the master of an element from the metadata. For single-home and lock-onlies, all masters
  // will be the same in the metadata
  return txn->internal().master_metadata().begin()->second.master();
}

const TxnIdReplicaIdPair TransactionHolder::transaction_id_replica_id() const {
  return transaction_id_replica_id(transaction());
}

const TxnIdReplicaIdPair TransactionHolder::transaction_id_replica_id(const Transaction* txn) {
  auto txn_id = txn->internal().id();
  auto local_log_id = replica_id(txn);
  return make_pair(txn_id, local_log_id);
}

} // namespace slog