#include "module/scheduler_components/per_key_remaster_manager.h"

#include <glog/logging.h>

using std::make_pair;

namespace slog {

PerKeyRemasterManager::PerKeyRemasterManager(
    shared_ptr<const Storage<Key, Record>> storage) : storage_(storage) {}

VerifyMasterResult
PerKeyRemasterManager::VerifyMaster(const TransactionHolder* txn_holder) {
  auto txn = txn_holder->GetTransaction();
  auto& keys = txn_holder->KeysInPartition();
  if (keys.empty()) {
    // None of the keys in this txn are in this partition
    return VerifyMasterResult::VALID;
  }

  auto& txn_master_metadata = txn->internal().master_metadata();
  // This should only be the case for testing
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
    InsertIntoBlockedQueue(key, counter, txn_holder);
  }
  return VerifyMasterResult::WAITING;
}

RemasterOccurredResult PerKeyRemasterManager::ReleaseTransaction(TxnId txn_id) {
  LOG(FATAL) << "Release not implemented";
}

RemasterOccurredResult PerKeyRemasterManager::ReleaseTransaction(const TransactionHolder* txn_holder) {
  auto txn = txn_holder->GetTransaction();
  auto txn_id = txn->internal().id();
  for (auto& key_pair : txn_holder->KeysInPartition()) {
    auto& key = key_pair.first;
    if (blocked_queue_.count(key) > 0) {
      auto& queue = blocked_queue_[key];
      for (auto itr = queue.begin(); itr != queue.end(); itr++) {
        if ((*itr).first->GetTransaction()->internal().id() == txn_id) {
          queue.erase(itr);
          break;
        }
      }
      if (queue.empty()) {
        blocked_queue_.erase(key);
      }
    }
  }

  RemasterOccurredResult result;
  for (auto& key_pair : txn_holder->KeysInPartition()) {
    auto& key = key_pair.first;
    TryToUnblock(key, result);
  }
  return result;
}

void PerKeyRemasterManager::InsertIntoBlockedQueue(const Key key, const uint32_t counter, const TransactionHolder* txn_holder) {
  auto entry = make_pair(txn_holder, counter);

  // Iterate until at end or counter is smaller than next element. Maintains priority queue
  auto itr = blocked_queue_[key].begin();
  while (itr != blocked_queue_[key].end() && (*itr).second <= counter) {
    itr++;
  }
  blocked_queue_[key].insert(itr, entry);
}

RemasterOccurredResult PerKeyRemasterManager::RemasterOccured(Key key, const uint32_t remaster_counter) {
  RemasterOccurredResult result;

  // No txns waiting for this remaster
  if (blocked_queue_.count(key) == 0) {
    return result;
  }

  // CHECK_EQ(remaster_counter, counters_.at(key) + 1) << "Remaster didn't increase cached counter by 1";
  counters_[key] = remaster_counter;

  if (blocked_queue_[key].front().second == remaster_counter) {
    TryToUnblock(key, result);
  }
  
  return result;
}

void PerKeyRemasterManager::TryToUnblock(Key unblocked_key, RemasterOccurredResult& result) {
  if (blocked_queue_.count(unblocked_key) == 0) {
    return;
  }

  auto& txn_pair = blocked_queue_.at(unblocked_key).front();
  auto& txn_holder = txn_pair.first;
  
  bool can_be_unblocked = true;
  bool should_abort = false;
  for (auto& key_pair : txn_holder->KeysInPartition()) {
    auto key = key_pair.first;
    CHECK(blocked_queue_.count(key) > 0) << "Transaction was not in correct blocked_queues";
    auto& front_txn_replica_id = blocked_queue_.at(key).front().first;
    if (front_txn_replica_id != txn_holder || // txn is not at front of this queue
          counters_.at(key) < blocked_queue_.at(key).front().second) { // txn is blocked on this key
      can_be_unblocked = false;
      if (counters_.at(key) > blocked_queue_.at(key).front().second) { 
        should_abort = true;
      }
      break;
    }
  }

  if (!can_be_unblocked) {
    return;
  }

  if (should_abort) {
    result.should_abort.push_back(txn_holder);
  } else {
    result.unblocked.push_back(txn_holder);
  }

  // Garbage collect queue and counters
  for (auto& key_pair : txn_holder->KeysInPartition()) {
    auto key = key_pair.first;
    blocked_queue_.at(key).pop_front();
    if (blocked_queue_.at(key).size() == 0) {
      blocked_queue_.erase(key);
      counters_.erase(key);
    }
  }

  // Recurse on each updated queue
  for (auto& key_pair : txn_holder->KeysInPartition()) {
    auto key = key_pair.first;
    TryToUnblock(key, result);
  }
}

} // namespace slog