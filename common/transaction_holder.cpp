#include "common/transaction_holder.h"
#include "common/proto_utils.h"

using std::pair;
using std::vector;
using std::string;

namespace slog {

namespace {

void ExtractKeysInPartition(
    vector<std::pair<Key, LockMode>>& keys,
    ConfigurationPtr config,
    const Transaction& txn) {
  for (const auto& kv : txn.read_set()) {
    // If this key is also in write_set, give it write lock instead
    if (config->KeyIsInLocalPartition(kv.first) 
        && !txn.write_set().contains(kv.first)) {
      keys.emplace_back(kv.first, LockMode::READ);
    }
  }
  for (const auto& kv : txn.write_set()) {
    if (config->KeyIsInLocalPartition(kv.first)) {
      keys.emplace_back(kv.first, LockMode::WRITE);
    }
  }
}

} // namespace

TransactionHolder::TransactionHolder() : txn_(nullptr), worker_("") {}
TransactionHolder::TransactionHolder(ConfigurationPtr config, Transaction* txn) : worker_("") {
  SetTransaction(config, txn);
}

TransactionHolder::~TransactionHolder() {
  delete txn_;
}

void TransactionHolder::SetTransaction(const ConfigurationPtr config, Transaction* txn) {
  ExtractKeysInPartition(keys_in_partition_, config, *txn);
  txn_ = txn;
}

Transaction* TransactionHolder::GetTransaction() const {
  return txn_;
}

Transaction* TransactionHolder::ReleaseTransaction() {
  auto tmp = txn_;
  txn_ = nullptr;
  return tmp;
}

bool TransactionHolder::AddLockOnlyTransaction(const ConfigurationPtr config, Transaction* lo_txn, TxnReplicaId& lo_txn_replica_id) {
  CHECK(txn_->internal().type() == TransactionType::MULTI_HOME) <<
      "Transaction " << txn_->internal().id() << " is not MULTI_HOME, LOCK_ONLY should not be added";
  CHECK(lo_txn->internal().type() == TransactionType::LOCK_ONLY) <<
      "Transaction " << lo_txn->internal().id() << " is not LOCK_ONLY";
  auto txn_replica_id = GetTransactionReplicaId(lo_txn);
  auto replica_id = txn_replica_id.second;
  CHECK(keys_per_replica_.count(replica_id) == 0) <<
      "Transaction " << txn_->internal().id() << " already had a lock only transaction added for replica " << replica_id;
  ExtractKeysInPartition(keys_per_replica_[replica_id], config, *lo_txn);
  if (keys_per_replica_[replica_id].empty()) {
    keys_per_replica_.erase(replica_id);
    return false;
  } else {
    lo_txn_replica_id = txn_replica_id;
    return true;
  }
}

void TransactionHolder::SetWorker(const string& worker) {
  worker_ = worker;
}

const string& TransactionHolder::GetWorker() const {
  return worker_;
}

const KeyList& TransactionHolder::KeysInPartition() const {
  return keys_in_partition_;
}

const unordered_map<ReplicaId, KeyList>& TransactionHolder::KeysPerReplica() const {
  return keys_per_replica_;
}

vector<internal::Request>& TransactionHolder::EarlyRemoteReads() {
  return early_remote_reads_;
}


} // namespace slog