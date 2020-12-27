#include "module/scheduler_components/per_key_remaster_manager.h"

#include <glog/logging.h>

using std::make_pair;

namespace slog {

PerKeyRemasterManager::PerKeyRemasterManager(const shared_ptr<const Storage<Key, Record>>& storage)
    : storage_(storage) {}

VerifyMasterResult PerKeyRemasterManager::VerifyMaster(const TransactionHolder* txn_holder) {
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

  switch (CheckCounters(txn_holder, storage_)) {
    case VerifyMasterResult::ABORT: {
      return VerifyMasterResult::ABORT;
    }
    case VerifyMasterResult::VALID: {
      // Check if other transactions are blocking the queue
      auto indirectly_blocked = false;
      for (auto& key_pair : keys) {
        auto q_it = blocked_queue_.find(key_pair.first);
        // If the queue is emtpy, can be skipped
        if (q_it != blocked_queue_.end() && !q_it->second.empty()) {
          // If we won't be at the front of the queue, the txn will need to wait.
          if (q_it->second.front().second <= txn_master_metadata.at(key_pair.first).counter()) {
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
          break;  // Txns should only occur in a queue once
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

void PerKeyRemasterManager::InsertIntoBlockedQueue(const Key& key, const uint32_t counter,
                                                   const TransactionHolder* txn_holder) {
  auto entry = make_pair(txn_holder, counter);

  // Iterate until at end or counter is smaller than next element. Maintains priority queue
  auto& q = blocked_queue_[key];
  auto itr = q.begin();
  while (itr != q.end() && (*itr).second <= counter /* note <= */) {
    itr++;
  }
  q.insert(itr, entry);
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
  auto q_it = blocked_queue_.find(unblocked_key);
  if (q_it == blocked_queue_.end()) {
    return;
  }

  auto& txn_pair = q_it->second.front();
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
        auto q_it = blocked_queue_.find(key_pair.first);
        DCHECK(q_it != blocked_queue_.end()) << "Transaction was not in correct blocked_queues";
        auto& front_txn_replica_id = q_it->second.front().first;
        if (front_txn_replica_id != txn_holder) {
          return;
        }
      }
      result.unblocked.push_back(txn_holder);

      // Garbage collect queue and counters
      for (auto& key_pair : txn_holder->keys_in_partition()) {
        auto q_it = blocked_queue_.find(key_pair.first);
        q_it->second.pop_front();
        if (q_it->second.size() == 0) {
          blocked_queue_.erase(key_pair.first);
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

}  // namespace slog