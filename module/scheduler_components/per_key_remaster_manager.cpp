#include "module/scheduler_components/per_key_remaster_manager.h"

#include <glog/logging.h>

using std::make_pair;

namespace slog {

PerKeyRemasterManager::PerKeyRemasterManager(
    const shared_ptr<const Storage<Key, Record>>& storage) : storage_(storage) {}

VerifyMasterResult
PerKeyRemasterManager::VerifyMaster(const TransactionHolder* txn_holder) {
  auto txn = txn_holder->transaction();
  auto& keys = txn_holder->keys_in_partition();
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

  switch(CheckCounters(txn_holder, storage_)) {
    case VerifyMasterResult::ABORT: {
      return VerifyMasterResult::ABORT;
    }
    case VerifyMasterResult::VALID: {
      // Check if other transactions are blocking the queue
      auto indirectly_blocked = false;
      for (auto& key_pair :keys) {
        auto key = key_pair.first;
        // If the queue is emtpy, can be skipped
        if (blocked_queue_.count(key)) {
          // If we won't be at the front of the queue, the txn will need to wait.
          if (blocked_queue_[key].front().second <= txn_master_metadata.at(key).counter()) {
            indirectly_blocked = true;
            break;
          }
        }
      }
      if (!indirectly_blocked) {
        return VerifyMasterResult::VALID;
      }
      break;
    }
    case VerifyMasterResult::WAITING: {
      break;
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

RemasterOccurredResult PerKeyRemasterManager::ReleaseTransaction(const TransactionHolder* txn_holder) {
  auto txn = txn_holder->transaction();
  auto txn_id = txn->internal().id();
  for (auto& key_pair : txn_holder->keys_in_partition()) {
    auto& key = key_pair.first;
    if (blocked_queue_.count(key) > 0) {
      auto& queue = blocked_queue_[key];
      for (auto itr = queue.begin(); itr != queue.end(); itr++) {
        if ((*itr).first->transaction()->internal().id() == txn_id) {
          queue.erase(itr);
          break; // Txns should only occur in a queue once
        }
      }
      if (queue.empty()) {
        blocked_queue_.erase(key);
      }
    }
  }

  RemasterOccurredResult result;
  for (auto& key_pair : txn_holder->keys_in_partition()) {
    auto& key = key_pair.first;
    TryToUnblock(key, result);
  }
  return result;
}

void PerKeyRemasterManager::InsertIntoBlockedQueue(const Key& key, const uint32_t counter, const TransactionHolder* txn_holder) {
  auto entry = make_pair(txn_holder, counter);

  // Iterate until at end or counter is smaller than next element. Maintains priority queue
  auto itr = blocked_queue_[key].begin();
  while (itr != blocked_queue_[key].end() && (*itr).second <= counter /* note <= */) {
    itr++;
  }
  blocked_queue_[key].insert(itr, entry);
}

RemasterOccurredResult PerKeyRemasterManager::RemasterOccured(const Key& key, const uint32_t /* remaster_counter */) {
  RemasterOccurredResult result;

  // No txns waiting for this remaster
  if (blocked_queue_.count(key) == 0) {
    return result;
  }

  TryToUnblock(key, result);

  return result;
}

void PerKeyRemasterManager::TryToUnblock(const Key& unblocked_key, RemasterOccurredResult& result) {
  if (blocked_queue_.count(unblocked_key) == 0) {
    return;
  }

  auto& txn_pair = blocked_queue_[unblocked_key].front();
  auto& txn_holder = txn_pair.first;

  switch (CheckCounters(txn_holder, storage_)) {
    case VerifyMasterResult::ABORT: {
      result.should_abort.push_back(txn_holder);
      auto release_result = ReleaseTransaction(txn_holder);
      // Newly unblocked txns are added to the end of the list
      result.unblocked.splice(result.unblocked.end(), release_result.unblocked);
      result.should_abort.splice(result.should_abort.end(), release_result.should_abort);
      return;
    }
    case VerifyMasterResult::WAITING: {
      return;
    }
    case VerifyMasterResult::VALID: {
      for (auto& key_pair : txn_holder->keys_in_partition()) {
        auto key = key_pair.first;
        CHECK(blocked_queue_.count(key) > 0) << "Transaction was not in correct blocked_queues";
        auto& front_txn_replica_id = blocked_queue_[key].front().first;
        if (front_txn_replica_id != txn_holder) {
          return;
        }
      }
      result.unblocked.push_back(txn_holder);

      // Garbage collect queue and counters
      for (auto& key_pair : txn_holder->keys_in_partition()) {
        auto key = key_pair.first;
        blocked_queue_[key].pop_front();
        if (blocked_queue_[key].size() == 0) {
          blocked_queue_.erase(key);
        }
      }

      // Recurse on each updated queue
      for (auto& key_pair : txn_holder->keys_in_partition()) {
        auto key = key_pair.first;
        TryToUnblock(key, result);
      }
      break;
    }
  }
}

} // namespace slog 