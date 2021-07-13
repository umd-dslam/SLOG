#include "common/proto_utils.h"

#include <glog/logging.h>

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <optional>
#include <sstream>
#include <unordered_set>

using std::string;
using std::vector;

namespace slog {

Transaction* MakeTransaction(const vector<KeyMetadata>& key_metadatas,
                             const std::vector<std::vector<std::string>>& code, std::optional<int> remaster,
                             MachineId coordinating_server) {
  Transaction* txn = new Transaction();
  for (const auto& key_metadata : key_metadatas) {
    auto new_entry = txn->mutable_keys()->Add();
    new_entry->set_key(key_metadata.key);
    auto new_value_entry = new_entry->mutable_value_entry();
    new_value_entry->set_type(key_metadata.type);
    if (key_metadata.metadata.has_value()) {
      new_value_entry->mutable_metadata()->set_master(key_metadata.metadata->master);
      new_value_entry->mutable_metadata()->set_counter(key_metadata.metadata->counter);
    }
  }
  if (remaster.has_value()) {
    txn->mutable_remaster()->set_new_master(remaster.value());
  } else {
    for (const auto& proc : code) {
      auto proc_proto = txn->mutable_code()->add_procedures();
      for (const auto& args : proc) {
        proc_proto->add_args(args);
      }
    }
  }
  txn->set_status(TransactionStatus::NOT_STARTED);
  txn->mutable_internal()->set_id(1000);
  txn->mutable_internal()->set_coordinating_server(coordinating_server);

  SetTransactionType(*txn);

  PopulateInvolvedReplicas(*txn);

  return txn;
}

TransactionType SetTransactionType(Transaction& txn) {
  auto txn_internal = txn.mutable_internal();

  bool master_metadata_is_complete = !txn.keys().empty();
  for (const auto& kv : txn.keys()) {
    if (!kv.value_entry().has_metadata()) {
      master_metadata_is_complete = false;
      break;
    }
  }

  if (!master_metadata_is_complete) {
    txn_internal->set_type(TransactionType::UNKNOWN);
    return txn_internal->type();
  }

  bool is_single_home = true;
  auto home = txn.keys().begin()->value_entry().metadata().master();
  for (const auto& kv : txn.keys()) {
    if (kv.value_entry().metadata().master() != home) {
      is_single_home = false;
      break;
    }
  }

#ifdef REMASTER_PROTOCOL_COUNTERLESS
  // Remaster txn will become multi-home
  if (txn.program_case() == Transaction::kRemaster) {
    is_single_home = false;
  }
#endif /* REMASTER_PROTOCOL_COUNTERLESS */
  if (is_single_home) {
    txn_internal->set_type(TransactionType::SINGLE_HOME);
    txn_internal->set_home(home);
  } else {
    txn_internal->set_type(TransactionType::MULTI_HOME_OR_LOCK_ONLY);
    txn_internal->set_home(-1);
  }
  return txn_internal->type();
}

Transaction* GenerateLockOnlyTxn(Transaction* txn, uint32_t lo_master, bool in_place) {
  Transaction* lock_only_txn = txn;
  if (!in_place) {
    lock_only_txn = new Transaction(*txn);
  }

  lock_only_txn->mutable_internal()->set_home(lo_master);

#ifdef REMASTER_PROTOCOL_COUNTERLESS
  if (lock_only_txn->program_case() == Transaction::kRemaster && lock_only_txn->remaster().new_master() == lo_master) {
    lock_only_txn->mutable_remaster()->set_is_new_master_lock_only(true);
    // For remaster txn, there is only one key in the metadata, and we want to keep that key there
    // in this case, so we return here.
    return lock_only_txn;
  }
#endif /* REMASTER_PROTOCOL_COUNTERLESS */

  return lock_only_txn;
}

Transaction* GeneratePartitionedTxn(const SharderPtr& sharder, Transaction* txn, uint32_t partition, bool in_place) {
  Transaction* new_txn = txn;
  if (!in_place) {
    new_txn = new Transaction(*txn);
  }

  vector<bool> involved_replicas(8, false);

  // Check if the generated subtxn does not intend to lock any key in its home region
  // If this is a remaster txn, it is never redundant
  bool is_redundant = !new_txn->has_remaster();

  // Remove keys that are not in the target partition
  for (auto it = new_txn->mutable_keys()->begin(); it != new_txn->mutable_keys()->end();) {
    if (sharder->compute_partition(it->key()) != partition) {
      it = new_txn->mutable_keys()->erase(it);
    } else {
      auto master = it->value_entry().metadata().master();
      if (master >= involved_replicas.size()) {
        involved_replicas.resize(master + 1);
      }
      involved_replicas[master] = true;
      is_redundant &= static_cast<int>(master) != new_txn->internal().home();

      ++it;
    }
  }

  // Shortcut for when the key set is empty or there is no key mastered at the home region
  if (new_txn->keys().empty() || is_redundant) {
    delete new_txn;
    return nullptr;
  }

  // Update involved replica list if needed
  if (new_txn->internal().type() == TransactionType::MULTI_HOME_OR_LOCK_ONLY) {
    new_txn->mutable_internal()->mutable_involved_replicas()->Clear();
    for (size_t r = 0; r < involved_replicas.size(); ++r) {
      if (involved_replicas[r]) {
        new_txn->mutable_internal()->add_involved_replicas(r);
      }
    }
  }

  return new_txn;
}

void PopulateInvolvedReplicas(Transaction& txn) {
  if (txn.internal().type() == TransactionType::UNKNOWN) {
    return;
  }

  if (txn.internal().type() == TransactionType::SINGLE_HOME) {
    txn.mutable_internal()->mutable_involved_replicas()->Clear();
    CHECK(txn.keys().begin()->value_entry().has_metadata());
    txn.mutable_internal()->add_involved_replicas(txn.keys().begin()->value_entry().metadata().master());
    return;
  }

  vector<uint32_t> involved_replicas;
  for (const auto& kv : txn.keys()) {
    CHECK(kv.value_entry().has_metadata());
    involved_replicas.push_back(kv.value_entry().metadata().master());
  }

#ifdef REMASTER_PROTOCOL_COUNTERLESS
  if (txn.program_case() == Transaction::kRemaster) {
    involved_replicas.push_back(txn.remaster().new_master());
  }
#endif

  sort(involved_replicas.begin(), involved_replicas.end());
  auto last = unique(involved_replicas.begin(), involved_replicas.end());
  txn.mutable_internal()->mutable_involved_replicas()->Add(involved_replicas.begin(), last);
}

void PopulateInvolvedPartitions(const SharderPtr& sharder, Transaction& txn) {
  vector<bool> involved_partitions(sharder->num_partitions(), false);
  vector<bool> active_partitions(sharder->num_partitions(), false);
  for (const auto& kv : txn.keys()) {
    auto partition = sharder->compute_partition(kv.key());
    involved_partitions[partition] = true;
    if (kv.value_entry().type() == KeyType::WRITE) {
      active_partitions[partition] = true;
    }
  }

  for (size_t p = 0; p < involved_partitions.size(); ++p) {
    if (involved_partitions[p]) {
      txn.mutable_internal()->add_involved_partitions(p);
    }
  }

  for (size_t p = 0; p < active_partitions.size(); ++p) {
    if (active_partitions[p]) {
      txn.mutable_internal()->add_active_partitions(p);
    }
  }
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
    std::unordered_set<std::string> existing_keys;
    for (const auto& kv : txn.keys()) {
      existing_keys.insert(kv.key());
    }
    for (const auto& kv : other.keys()) {
      if (existing_keys.find(kv.key()) == existing_keys.end()) {
        txn.mutable_keys()->Add()->CopyFrom(kv);
      }
    }
  }

  txn.mutable_internal()->mutable_events()->MergeFrom(other.internal().events());

  auto involved_replicas = txn.mutable_internal()->mutable_involved_replicas();
  involved_replicas->MergeFrom(other.internal().involved_replicas());
  std::sort(involved_replicas->begin(), involved_replicas->end());
  involved_replicas->erase(std::unique(involved_replicas->begin(), involved_replicas->end()), involved_replicas->end());
}

std::ostream& operator<<(std::ostream& os, const Procedures& code) {
  for (const auto& p : code.procedures()) {
    for (const auto& arg : p.args()) {
      os << arg << " ";
    }
    os << "\n";
  }
  return os;
}

std::string ToReadable(const std::string& s) {
  std::ostringstream ss;
  for (auto c : s) {
    if (std::isprint(c)) {
      ss << c;
    } else {
      ss << '\\' << std::hex << (int(c) & 0xff);
    }
  }
  return ss.str();
}

std::ostream& operator<<(std::ostream& os, const Transaction& txn) {
  os << "Transaction ID: " << txn.internal().id() << "\n";
  os << "Status: " << ENUM_NAME(txn.status(), TransactionStatus) << "\n";
  if (txn.status() == TransactionStatus::ABORTED) {
    os << "Abort reason: " << txn.abort_reason() << "\n";
  }
  os << "Key set:\n";
  os << std::setfill(' ');
  for (const auto& kv : txn.keys()) {
    const auto& k = kv.key();
    const auto& v = kv.value_entry();
    os << "[" << ENUM_NAME(v.type(), KeyType) << "] " << ToReadable(k) << "\n";
    os << "\tValue: " << ToReadable(v.value()) << "\n";
    if (v.type() == KeyType::WRITE) {
      os << "\tNew value: " << ToReadable(v.new_value()) << "\n";
    }
    os << "\tMetadata: " << v.metadata() << "\n";
  }
  if (!txn.deleted_keys().empty()) {
    os << "Deleted keys: ";
    for (const auto& k : txn.deleted_keys()) {
      os << "\t" << ToReadable(k) << "\n";
    }
  }
  os << "Type: " << ENUM_NAME(txn.internal().type(), TransactionType) << "\n";
  if (txn.program_case() == Transaction::ProgramCase::kCode) {
    os << "Code:\n" << txn.code();
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

std::ostream& operator<<(std::ostream& os, const MasterMetadata& metadata) {
  os << "(" << metadata.master() << ", " << metadata.counter() << ")";
  return os;
}

bool operator==(const MasterMetadata& metadata1, const MasterMetadata& metadata2) {
  return metadata1.master() == metadata2.master() && metadata1.counter() == metadata2.counter();
}

bool operator==(const ValueEntry& val1, const ValueEntry& val2) {
  return val1.value() == val2.value() && val1.new_value() == val2.new_value() && val1.type() == val2.type() &&
         val1.metadata() == val2.metadata();
}

bool operator==(const KeyValueEntry& kv1, const KeyValueEntry& kv2) {
  return kv1.key() == kv2.key() && kv1.value_entry() == kv2.value_entry();
}

bool operator==(const Transaction& txn1, const Transaction txn2) {
  bool ok = txn1.status() == txn2.status() && txn1.keys_size() == txn2.keys_size() &&
            txn1.program_case() == txn2.program_case() && txn1.abort_reason() == txn2.abort_reason() &&
            txn1.internal().id() == txn2.internal().id() && txn1.internal().type() == txn2.internal().type();
  if (!ok) {
    return false;
  }
  for (int i = 0; i < txn1.keys_size(); i++) {
    if (!(txn1.keys(i) == txn2.keys(i))) {
      return false;
    }
  }
  return true;
}

vector<Transaction*> Unbatch(internal::Batch* batch) {
  auto transactions = batch->mutable_transactions();

  vector<Transaction*> buffer(transactions->size());

  for (int i = transactions->size() - 1; i >= 0; i--) {
    auto txn = transactions->ReleaseLast();
    auto txn_internal = txn->mutable_internal();

    // Transfer recorded events from batch to each txn in the batch
    txn_internal->mutable_events()->MergeFrom(batch->events());

    buffer[i] = txn;
  }

  return buffer;
}

}  // namespace slog