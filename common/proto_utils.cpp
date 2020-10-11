#include "common/proto_utils.h"

#include <iomanip>
#include <iostream>
#include <sstream>

using std::string;

namespace slog {

using internal::MachineId;

namespace {

using google::protobuf::Map;

bool operator!=(MasterMetadata metadata1, MasterMetadata metadata2) {
  return metadata1.master() != metadata2.master() 
      || metadata1.counter() != metadata2.counter();
}

template<typename K, typename V>
bool operator==(Map<K, V> map1, Map<K, V> map2) {
  if (map1.size() != map2.size()) {
    return false;
  }
  for (const auto& key_value : map1) {
    auto& key = key_value.first;
    auto& value = key_value.second;
    if (!map2.contains(key) || map2.at(key) != value) {
      return false;
    }
  }
  return true;
}

} // namespace


MachineId MakeMachineId(uint32_t replica, uint32_t partition) {
  MachineId machine_id;  
  machine_id.set_replica(replica);
  machine_id.set_partition(partition);
  return machine_id;
}

MachineId MakeMachineId(const string& machine_id_str) {
  auto split = machine_id_str.find(':');
  if (split == string::npos) {
    throw std::invalid_argument("Invalid machine id: " + machine_id_str);
  }
  try {
    auto replica_str = machine_id_str.substr(0, split);
    auto partition_str = machine_id_str.substr(split + 1);

    MachineId machine_id;
    machine_id.set_replica(std::stoul(replica_str));
    machine_id.set_partition(std::stoul(partition_str));
    return machine_id;
  } catch (...) {
    throw std::invalid_argument("Invalid machine id: " + machine_id_str);
  }
}

string MakeMachineIdAsString(uint32_t replica, uint32_t partition) {
  return std::to_string(replica) + ":" + std::to_string(partition);
}

string MakeMachineIdAsString(const MachineId& machine_id) {
  return MakeMachineIdAsString(machine_id.replica(), machine_id.partition());
}

Transaction* MakeTransaction(
    const unordered_set<Key>& read_set,
    const unordered_set<Key>& write_set,
    const string& code,
    const unordered_map<Key, pair<uint32_t, uint32_t>>& master_metadata,
    const internal::MachineId coordinating_server,
    const int32_t new_master) {
  Transaction* txn = new Transaction();
  for (const auto& key : read_set) {
    txn->mutable_read_set()->insert({key, ""});
  }
  for (const auto& key : write_set) {
    txn->mutable_write_set()->insert({key, ""});
  }
  if (new_master >= 0) { // Not set as default
    txn->mutable_remaster()->set_new_master(new_master);
  } else {
    txn->set_code(code);
  }
  txn->set_status(TransactionStatus::NOT_STARTED);

  for (const auto& pair : master_metadata) {
    const auto& key = pair.first;
    if (read_set.count(key) > 0 || write_set.count(key) > 0) {
      MasterMetadata metadata;
      metadata.set_master(pair.second.first);
      metadata.set_counter(pair.second.second);
      txn->mutable_internal()
          ->mutable_master_metadata()
          ->insert({pair.first, std::move(metadata)});
    }
  }
  txn->mutable_internal()
      ->mutable_coordinating_server()
      ->CopyFrom(coordinating_server);

  SetTransactionType(*txn);
  return txn;
}

TransactionType SetTransactionType(Transaction& txn) {
  auto txn_internal = txn.mutable_internal();
  auto& master_metadata = txn_internal->master_metadata();

  bool all_master_metadata_received = true;
  for (auto& pair : txn.read_set()) {
    if (!master_metadata.contains(pair.first)) {
      all_master_metadata_received = false;
      break;
    }
  }
  for (auto& pair : txn.write_set()) {
    if (!master_metadata.contains(pair.first)) {
      all_master_metadata_received = false;
      break;
    }
  }

  if (!all_master_metadata_received) {
    txn_internal->set_type(TransactionType::UNKNOWN);
    return txn_internal->type();
  }

  bool is_single_home = true;
  // If this is a single-home txn, home of all other keys must
  // be the same as that of the first key
  // TODO: check and report empty transaction
  const auto& home_replica = master_metadata.begin()->second.master();
  for (const auto& pair : master_metadata) {
    if (pair.second.master() != home_replica) {
      is_single_home = false;
      break;
    }
  }

#ifdef REMASTER_PROTOCOL_COUNTERLESS
  // Remaster txn will become multi-home
  if (txn.procedure_case() == Transaction::kRemaster) {
    is_single_home = false;
  }
#endif /* REMASTER_PROTOCOL_COUNTERLESS */

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
  
  if (other.status() == TransactionStatus::ABORTED) {
    txn.set_status(TransactionStatus::ABORTED);
    txn.set_abort_reason(other.abort_reason());
  } else if (txn.status() != TransactionStatus::ABORTED) {
    auto MergeMap = [](auto this_set, const auto& other_set) {
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
    MergeMap(txn.mutable_read_set(), other.read_set());
    MergeMap(txn.mutable_write_set(), other.write_set());
    txn.mutable_delete_set()->MergeFrom(other.delete_set());
  }

  txn.mutable_internal()
      ->mutable_events()
      ->MergeFrom(other.internal().events());
  txn.mutable_internal()
      ->mutable_event_times()
      ->MergeFrom(other.internal().event_times());
  txn.mutable_internal()
      ->mutable_event_machines()
      ->MergeFrom(other.internal().event_machines());
}

std::ostream& operator<<(std::ostream& os, const Transaction& txn) {
  os << "Transaction ID: " << txn.internal().id() << "\n";
  os << "Status: " 
      << ENUM_NAME(txn.status(), TransactionStatus) << "\n";
  os << "Read set:" << "\n";
  os << std::setfill(' ');
  for (const auto& pair : txn.read_set()) {
    os << std::setw(10) << pair.first << " ==> " << pair.second << "\n";
  }
  os << "Write set:\n";
  for (const auto& pair : txn.write_set()) {
    os << std::setw(10) << pair.first << " ==> " << pair.second << "\n";
  }
  os << "Master metadata:\n";
  for (const auto& pair : txn.internal().master_metadata()) {
    os << std::setw(10) << pair.first << ": " << pair.second << ")\n";
  }
  os << "Type: "
      << ENUM_NAME(txn.internal().type(), TransactionType) << "\n";
  if (txn.procedure_case() == Transaction::ProcedureCase::kCode) {
    os << "Code: " << txn.code() << std::endl;
  } else {
    os << "New master: " << txn.remaster().new_master() << std::endl;
  }
  return os;
}

bool operator==(const Transaction& txn1, const Transaction txn2) {
  return txn1.status() == txn2.status()
      && txn1.read_set() == txn2.read_set()
      && txn1.write_set() == txn2.write_set()
      && txn1.procedure_case() == txn2.procedure_case()
      && txn1.abort_reason() == txn2.abort_reason()
      && txn1.internal().id() == txn2.internal().id()
      && txn1.internal().master_metadata() == txn2.internal().master_metadata()
      && txn1.internal().type() == txn2.internal().type();
}

std::ostream& operator<<(std::ostream& os, const MasterMetadata& metadata) {
  os << std::setw(10) << "(" << metadata.master() << ", " << metadata.counter() << ")";
  return os;
}

} // namespace slog