#include "common/proto_utils.h"

#include <iostream>
#include <sstream>

namespace slog {

Transaction MakeTransaction(
    const unordered_set<Key>& read_set,
    const unordered_set<Key>& write_set,
    const string& code,
    const unordered_map<Key, pair<uint32_t, uint32_t>>& master_metadata,
    const internal::MachineId coordinating_server) {
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
  txn.mutable_internal()
      ->mutable_coordinating_server()
      ->CopyFrom(coordinating_server);

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

void MergeTransaction(Transaction& txn, const Transaction& other) {
  if (txn.internal().id() != other.internal().id()) {
    std::ostringstream oss;
    oss << "Cannot merge transactions with different IDs: "
        << txn.internal().id() << " vs. " << other.internal().id();
    throw std::runtime_error(oss.str());
  }
  if (txn.internal().type() != other.internal().type()) {
    std::ostringstream oss;
    oss << "Cannot merge transactions with different types: "
        << txn.internal().type() << " vs. " << other.internal().type();
    throw std::runtime_error(oss.str());
  }
  
  auto MergeSet = [](auto this_set, const auto& other_set) {
    for (const auto& key_value : other_set) {
      const auto& key = key_value.first;
      const auto& value = key_value.second;
      if (this_set->contains(key)) {
        if (this_set->at(key) != value) {
          std::ostringstream oss;
          oss << "Found conflicting value at key \"" << key << "\" while merging transactions. Val: "
              << this_set->at(key) << ". Other val: " << value;
          throw std::runtime_error(oss.str());
        }
      } else {
        this_set->insert(key_value);
      }
    }
  };
  MergeSet(txn.mutable_read_set(), other.read_set());
  MergeSet(txn.mutable_write_set(), other.write_set());

  txn.mutable_delete_set()->MergeFrom(other.delete_set());
  
  if (txn.status() != TransactionStatus::ABORTED) {
    txn.set_status(other.status());
  }
  txn.set_abort_reason(other.abort_reason());
}

using std::endl;

std::ostream& operator<<(std::ostream& os, const Transaction& txn) {
  os << "Transaction ID: " << txn.internal().id() << endl;
  os << "Status: ";
  switch (txn.status()) {
    case TransactionStatus::ABORTED:
        os << "ABORTED" << endl;
        os << "Abort reason: " << txn.abort_reason() << endl;
        break;
    case TransactionStatus::COMMITTED: os << "COMMITTED" << endl; break;
    case TransactionStatus::NOT_STARTED: os << "NOT_STARTED" << endl; break;
    default:
      break;
  }
  os << "Read set:" << endl;
  os << std::setfill(' ');
  for (const auto& pair : txn.read_set()) {
    os << std::setw(10) << pair.first << " ==> " << pair.second << endl;
  }
  os << "Write set:" << endl;
  for (const auto& pair : txn.write_set()) {
    os << std::setw(10) << pair.first << " ==> " << pair.second << endl;
  }
  os << "Code: " << txn.code() << endl;
  return os;
}

} // namespace slog