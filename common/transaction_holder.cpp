#include "common/transaction_holder.h"

#include <glog/logging.h>

using std::pair;
using std::make_pair;
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

void TransactionHolder::SetWorker(const string& worker) {
  worker_ = worker;
}

const string& TransactionHolder::GetWorker() const {
  return worker_;
}

const vector<pair<Key, LockMode>>& TransactionHolder::KeysInPartition() const {
  return keys_in_partition_;
}

vector<internal::Request>& TransactionHolder::EarlyRemoteReads() {
  return early_remote_reads_;
}

const uint32_t TransactionHolder::GetReplicaId() const {
  return GetReplicaId(txn_);
}

const uint32_t TransactionHolder::GetReplicaId(Transaction* txn) {
  if (txn->internal().master_metadata().empty()) { // This should only be the case for testing
    LOG(WARNING) << "Master metadata empty: txn id " << txn->internal().id();
    return 0;
  }
  return txn->internal().master_metadata().begin()->second.master();
}

const TxnIdReplicaIdPair TransactionHolder::GetTransactionIdReplicaIdPair() const {
  return GetTransactionIdReplicaIdPair(txn_);
}

const TxnIdReplicaIdPair TransactionHolder::GetTransactionIdReplicaIdPair(Transaction* txn) {
  auto txn_id = txn->internal().id();
  auto local_log_id = GetReplicaId(txn);
  return make_pair(txn_id, local_log_id);
}

} // namespace slog