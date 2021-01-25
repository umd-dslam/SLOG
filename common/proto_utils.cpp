#include "common/proto_utils.h"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <optional>
#include <sstream>

using std::get_if;
using std::string;
namespace slog {

namespace {

using google::protobuf::Map;

bool operator!=(MasterMetadata metadata1, MasterMetadata metadata2) {
  return metadata1.master() != metadata2.master() || metadata1.counter() != metadata2.counter();
}

template <typename K, typename V>
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

}  // namespace

Transaction* MakeTransaction(const vector<Key>& read_set, const vector<Key>& write_set,
                             const std::variant<string, int>& proc,
                             const unordered_map<Key, pair<uint32_t, uint32_t>>& master_metadata,
                             MachineId coordinating_server) {
  Transaction* txn = new Transaction();
  for (const auto& key : read_set) {
    txn->mutable_read_set()->insert({key, ""});
  }
  for (const auto& key : write_set) {
    txn->mutable_write_set()->insert({key, ""});
  }
  if (auto code = get_if<string>(&proc); code) {
    txn->set_code(*code);
  } else {
    txn->mutable_remaster()->set_new_master(std::get<int>(proc));
  }
  txn->set_status(TransactionStatus::NOT_STARTED);
  txn->mutable_internal()->set_id(1000);
  txn->mutable_internal()->set_coordinating_server(coordinating_server);

  for (const auto& pair : master_metadata) {
    const auto& key = pair.first;
    if (txn->read_set().count(key) > 0 || txn->write_set().count(key) > 0) {
      MasterMetadata metadata;
      metadata.set_master(pair.second.first);
      metadata.set_counter(pair.second.second);
      txn->mutable_internal()->mutable_master_metadata()->insert({pair.first, std::move(metadata)});
    }
  }

  SetTransactionType(*txn);

  PopulateInvolvedReplicas(txn);

  return txn;
}

Transaction* GenerateLockOnlyTxn(Transaction& txn, uint32_t lo_master, bool in_place) {
  Transaction* lock_only_txn = &txn;
  if (!in_place) {
    lock_only_txn = new Transaction(txn);
  }

#ifdef REMASTER_PROTOCOL_COUNTERLESS
  if (txn.procedure_case() == Transaction::kRemaster && txn.remaster().new_master() == lo_master) {
    lock_only_txn->mutable_remaster()->set_is_new_master_lock_only(true);
    // For remaster txn, there is only one key in the metadata, and we want to keep that key there
    // in this case, so we return here.
    return lock_only_txn;
  }
#endif /* REMASTER_PROTOCOL_COUNTERLESS */

  auto master_metadata = lock_only_txn->mutable_internal()->mutable_master_metadata();
  for (auto it = master_metadata->begin(); it != master_metadata->end();) {
    if (it->second.master() != lo_master) {
      it = master_metadata->erase(it);
    } else {
      ++it;
    }
  }

  return lock_only_txn;
}

void PopulateInvolvedReplicas(Transaction* txn) {
  if (txn->internal().type() == TransactionType::UNKNOWN) {
    return;
  }

  if (txn->internal().type() == TransactionType::SINGLE_HOME) {
    txn->mutable_internal()->mutable_involved_replicas()->Clear();
    txn->mutable_internal()->add_involved_replicas(txn->internal().master_metadata().begin()->second.master());
    return;
  }

  vector<uint32_t> involved_replicas;
  for (const auto& pair : txn->internal().master_metadata()) {
    involved_replicas.push_back(pair.second.master());
  }
#ifdef REMASTER_PROTOCOL_COUNTERLESS
  if (txn->procedure_case() == Transaction::kRemaster) {
    involved_replicas.push_back(txn->remaster().new_master());
  }
#endif

  sort(involved_replicas.begin(), involved_replicas.end());
  auto last = unique(involved_replicas.begin(), involved_replicas.end());
  *txn->mutable_internal()->mutable_involved_replicas() = {involved_replicas.begin(), last};
}

void PopulateInvolvedPartitions(Transaction* txn, const ConfigurationPtr& config) {
  vector<uint32_t> involved_partitions;
  auto ExtractPartitions = [&involved_partitions, &config](const google::protobuf::Map<string, string>& keys) {
    for (auto& pair : keys) {
      const auto& key = pair.first;
      uint32_t partition;
      partition = config->partition_of_key(key);
      involved_partitions.push_back(partition);
    }
  };
  ExtractPartitions(txn->read_set());
  ExtractPartitions(txn->write_set());

  sort(involved_partitions.begin(), involved_partitions.end());
  auto last = unique(involved_partitions.begin(), involved_partitions.end());
  *txn->mutable_internal()->mutable_involved_partitions() = {involved_partitions.begin(), last};
}

TransactionType SetTransactionType(Transaction& txn) {
  auto txn_internal = txn.mutable_internal();
  auto& master_metadata = txn_internal->master_metadata();

  bool master_metadata_is_complete = (txn.read_set_size() + txn.write_set_size()) > 0;
  for (auto& pair : txn.read_set()) {
    if (!master_metadata.contains(pair.first)) {
      master_metadata_is_complete = false;
      break;
    }
  }
  if (master_metadata_is_complete) {
    for (auto& pair : txn.write_set()) {
      if (!master_metadata.contains(pair.first)) {
        master_metadata_is_complete = false;
        break;
      }
    }
  }

  if (!master_metadata_is_complete) {
    txn_internal->set_type(TransactionType::UNKNOWN);
    return txn_internal->type();
  }

  bool is_single_home = true;
  std::optional<uint32_t> first_master;
  for (const auto& pair : master_metadata) {
    if (first_master.has_value() && pair.second.master() != first_master.value()) {
      is_single_home = false;
      break;
    }
    first_master = pair.second.master();
  }

#ifdef REMASTER_PROTOCOL_COUNTERLESS
  // Remaster txn will become multi-home
  if (txn.procedure_case() == Transaction::kRemaster) {
    is_single_home = false;
  }
#endif /* REMASTER_PROTOCOL_COUNTERLESS */

  txn_internal->set_type(is_single_home ? TransactionType::SINGLE_HOME : TransactionType::MULTI_HOME_OR_LOCK_ONLY);
  return txn_internal->type();
}

void MergeTransaction(Transaction& txn, const Transaction& other) {
  if (txn.internal().id() != other.internal().id()) {
    std::ostringstream oss;
    oss << "Cannot merge transactions with different IDs: " << txn.internal().id() << " vs. " << other.internal().id();
    throw std::runtime_error(oss.str());
  }
  if (txn.internal().type() != other.internal().type()) {
    std::ostringstream oss;
    oss << "Cannot merge transactions with different types: " << txn.internal().type() << " vs. "
        << other.internal().type();
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
            oss << "Found conflicting value at key \"" << key
                << "\" while merging transactions. Val: " << this_set->at(key) << ". Other val: " << value;
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

  txn.mutable_internal()->mutable_events()->MergeFrom(other.internal().events());
  txn.mutable_internal()->mutable_event_times()->MergeFrom(other.internal().event_times());
  txn.mutable_internal()->mutable_event_machines()->MergeFrom(other.internal().event_machines());
}

std::ostream& operator<<(std::ostream& os, const Transaction& txn) {
  os << "Transaction ID: " << txn.internal().id() << "\n";
  os << "Status: " << ENUM_NAME(txn.status(), TransactionStatus) << "\n";
  os << "Read set:"
     << "\n";
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
  os << "Type: " << ENUM_NAME(txn.internal().type(), TransactionType) << "\n";
  if (txn.procedure_case() == Transaction::ProcedureCase::kCode) {
    os << "Code: " << txn.code() << "\n";
  } else {
    os << "New master: " << txn.remaster().new_master() << "\n";
  }
  os << "Coordinating server: " << txn.internal().coordinating_server() << "\n";
  os << "Involved partitions: ";
  for (auto p : txn.internal().involved_partitions()) {
    os << p << " ";
  }
  os << "\n";
  os << "Involved replicas: ";
  for (auto r : txn.internal().involved_replicas()) {
    os << r << " ";
  }
  os << std::endl;
  return os;
}

bool operator==(const Transaction& txn1, const Transaction txn2) {
  return txn1.status() == txn2.status() && txn1.read_set() == txn2.read_set() && txn1.write_set() == txn2.write_set() &&
         txn1.procedure_case() == txn2.procedure_case() && txn1.abort_reason() == txn2.abort_reason() &&
         txn1.internal().id() == txn2.internal().id() &&
         txn1.internal().master_metadata() == txn2.internal().master_metadata() &&
         txn1.internal().type() == txn2.internal().type();
}

std::ostream& operator<<(std::ostream& os, const MasterMetadata& metadata) {
  os << std::setw(10) << "(" << metadata.master() << ", " << metadata.counter() << ")";
  return os;
}

vector<Transaction*> Unbatch(internal::Batch* batch) {
  auto transactions = batch->mutable_transactions();

  vector<Transaction*> buffer(transactions->size());

  for (int i = transactions->size() - 1; i >= 0; i--) {
    auto txn = transactions->ReleaseLast();
    auto txn_internal = txn->mutable_internal();

    // Transfer recorded events from batch to each txn in the batch
    txn_internal->mutable_events()->MergeFrom(batch->events());
    txn_internal->mutable_event_times()->MergeFrom(batch->event_times());
    txn_internal->mutable_event_machines()->MergeFrom(batch->event_machines());

    buffer[i] = txn;
  }

  return buffer;
}

}  // namespace slog