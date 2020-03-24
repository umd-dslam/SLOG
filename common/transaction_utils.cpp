#include "common/transaction_utils.h"

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


} // namespace slog