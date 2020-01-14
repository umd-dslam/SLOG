#include "common/proto_utils.h"

namespace slog {

Transaction MakeTransaction(
    const unordered_set<Key>& read_set,
    const unordered_set<Key>& write_set,
    const string& code,
    const unordered_map<Key, pair<uint32_t, uint32_t>>& master_metadata) {
  Transaction txn;
  for (const auto& key : read_set) {
    txn.mutable_read_set()->insert({key, ""});
  }
  for (const auto& key : write_set) {
    txn.mutable_write_set()->insert({key, ""});
  }
  txn.set_code(code);
  txn.set_status(TransactionStatus::NOT_STARTED);

  for (const auto& pair : master_metadata) {
    const auto& key = pair.first;
    if (read_set.count(key) > 0 || write_set.count(key) > 0) {
      MasterMetadata metadata;
      metadata.set_master(pair.second.first);
      metadata.set_counter(pair.second.second);
      txn.mutable_internal()
        ->mutable_master_metadata()
        ->insert({pair.first, std::move(metadata)});
    }
  }
  SetTransactionType(txn);
  return txn;
}

TransactionType SetTransactionType(Transaction& txn) {
  auto txn_internal = txn.mutable_internal();
  auto& txn_master_metadata = txn_internal->master_metadata();
  auto total_num_keys = static_cast<size_t>(
      txn.read_set_size() + txn.write_set_size());

  if (txn_master_metadata.size() != total_num_keys) {
    txn_internal->set_type(TransactionType::UNKNOWN);
    return txn_internal->type();
  }

  bool is_single_home = true;
  // Get master of the first key. If this is a single-home txn, it should
  // be the same for all keys
  const auto& home_replica = txn_master_metadata.begin()->second.master();
  for (const auto& pair : txn_master_metadata) {
    if (pair.second.master() != home_replica) {
      is_single_home = false;
      break;
    }
  }
  txn_internal->set_type(
      is_single_home ? TransactionType::SINGLE_HOME : TransactionType::MULTI_HOME);
  return txn_internal->type();
}

} // namespace slog