#include "module/scheduler_components/remaster_manager.h"

#include <glog/logging.h>

using std::make_pair;

namespace slog {

RemasterManager::RemasterManager(
    ConfigurationPtr config,
    shared_ptr<Storage<Key, Record>> storage,
    shared_ptr<TransactionMap> all_txns)
  : config_(config), storage_(storage), all_txns_(all_txns) {}

VerifyMasterResult
RemasterManager::VerifyMaster(const TxnReplicaId txn_replica_id) {
  auto& txn_holder = all_txns_->at(txn_replica_id);
  auto txn = txn_holder.GetTransaction();
  auto& keys = txn_holder.KeysInPartition();
  if (keys.empty()) {
    // None of the keys in this txn is in this partition
    return VerifyMasterResult::VALID;
  }

  auto& txn_master_metadata = txn->internal().master_metadata();
  // This should only be the case for testing
  // TODO: add metadata to test cases, make this fatal
  if (txn_master_metadata.empty()) {
    LOG(WARNING) << "Master metadata empty: txn id " << txn->internal().id();
    return VerifyMasterResult::VALID;
  }

  // keeps track of counters that will need to be waited on
  std::unordered_map<Key, int32_t> ahead_counters;
  for (auto& pair : keys) {
    auto key = pair.first;

    // Check with storage to get current counter
    if (counters_.count(key) == 0) {
      Record record;
      bool found = storage_->Read(key, record);
      if (found) {
        CHECK(txn_master_metadata.contains(key))
              << "Master metadata for key \"" << key << "\" is missing";
        counters_[key] = record.metadata.counter;
      } else {
        counters_[key] = 0;
      }
    }
    auto stored_counter = counters_.at(key);
    auto txn_metadata = txn_master_metadata.at(key);
    if (txn_metadata.counter() > stored_counter) {
      ahead_counters[key] = txn_metadata.counter();
    } else if (txn_metadata.counter() < stored_counter) {
      return VerifyMasterResult::ABORT;
    }
  }

  // Check if no keys are blocked at all
  if (ahead_counters.empty()){
    auto no_key_indirectly_blocked = true;
    for (auto& pair : keys) {
      auto key = pair.first;
      // If the queue is emtpy, can be skipped
      if (blocked_queue_.count(key)) {
        // If we won't be at the front of the queue, the txn will need to wait.
        if (blocked_queue_.at(key).front().second <= txn_master_metadata.at(key).counter()) {
          no_key_indirectly_blocked = false;
          break;
        }
      }
    }
    if (no_key_indirectly_blocked) {
      return VerifyMasterResult::VALID;
    }
  }

  // add the txn to the queue for each key
  for (auto& pair : keys) {
    auto key = pair.first;
    auto counter = txn_master_metadata.at(key).counter();
    InsertIntoBlockedQueue(key, counter, txn_replica_id);
  }
  return VerifyMasterResult::WAITING;
}

void RemasterManager::InsertIntoBlockedQueue(const Key key, const uint32_t counter, const TxnReplicaId txn_replica_id) {
  auto entry = make_pair(txn_replica_id, counter);

  // Iterate until at end or counter is smaller than next element. Maintains priority queue
  auto itr = blocked_queue_[key].begin();
  while (itr != blocked_queue_[key].end() && (*itr).second <= counter) {
    itr++;
  }
  blocked_queue_[key].insert(itr, entry);
}

list<TxnReplicaId> RemasterManager::RemasterOccured(Key key, const uint32_t remaster_counter) {
  list<TxnReplicaId> unblocked;
  // list<TxnId> shouldAbort;

  // No txns waiting for this remaster
  if (blocked_queue_.count(key) == 0) {
    return unblocked;
  }

  CHECK(remaster_counter == counters_.at(key) + 1) << "Remaster didn't increase cached counter by 1";
  counters_[key] = remaster_counter;

  if (blocked_queue_[key].front().second == remaster_counter) {
    TryToUnblock(key, unblocked);
  }
  
  return unblocked;
}

void RemasterManager::TryToUnblock(const Key unblocked_key, list<TxnReplicaId>& unblocked) {
  if (blocked_queue_.count(unblocked_key) == 0) {
    return;
  }

  auto& txn_pair = blocked_queue_.at(unblocked_key).front();
  auto& txn_replica_id = txn_pair.first;
  auto& txn_holder = all_txns_->at(txn_replica_id);
  
  bool can_be_unblocked = true;
  for (auto& key_pair : txn_holder.KeysInPartition()) {
    auto key = key_pair.first;
    CHECK(blocked_queue_.count(key) > 0) << "Transaction was not in correct blocked_queues";
    auto& front_txn_replica_id = blocked_queue_.at(key).front().first;
    if (front_txn_replica_id != txn_replica_id || // txn is not at front of this queue
          counters_.at(key) < blocked_queue_.at(key).front().second) { // txn is blocked on this key
      can_be_unblocked = false;
      break;
    }
  }

  if (!can_be_unblocked) {
    return;
  }

  unblocked.push_back(txn_replica_id);

  // Garbage collect queue and counters
  for (auto& key_pair : txn_holder.KeysInPartition()) {
    auto key = key_pair.first;
    blocked_queue_.at(key).pop_front();
    if (blocked_queue_.at(key).size() == 0) {
      blocked_queue_.erase(key);
      counters_.erase(key);
    }
  }

  // Recurse on each updated queue
  for (auto& key_pair : txn_holder.KeysInPartition()) {
    auto key = key_pair.first;
    TryToUnblock(key, unblocked);
  }
}

} // namespace slog