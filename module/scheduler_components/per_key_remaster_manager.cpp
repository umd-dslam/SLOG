#include "module/scheduler_components/per_key_remaster_manager.h"

#include <glog/logging.h>

using std::make_pair;

namespace slog {

PerKeyRemasterManager::PerKeyRemasterManager(const shared_ptr<const Storage<Key, Record>>& storage)
    : storage_(storage) {}

VerifyMasterResult PerKeyRemasterManager::VerifyMaster(const LockOnlyTxn& lo_txn) {
  auto& keys = lo_txn.keys;
  if (keys.empty()) {
    // None of the keys in this txn are in this partition
    return VerifyMasterResult::VALID;
  }

  switch (CheckCounters(lo_txn, storage_)) {
    case VerifyMasterResult::ABORT: {
      return VerifyMasterResult::ABORT;
    }
    case VerifyMasterResult::VALID: {
      // Check if other transactions are blocking the queue
      auto indirectly_blocked = false;
      for (auto& key_info : lo_txn.keys) {
        auto q_it = blocked_queue_.find(key_info.key);
        // If the queue is emtpy, can be skipped
        if (q_it != blocked_queue_.end() && !q_it->second.empty()) {
          // If we won't be at the front of the queue, the txn will need to wait.
          if (q_it->second.front().second <= key_info.counter) {
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
  for (auto& key_info : lo_txn.keys) {
    InsertIntoBlockedQueue(key_info.key, key_info.counter, lo_txn);
  }
  return VerifyMasterResult::WAITING;
}

RemasterOccurredResult PerKeyRemasterManager::ReleaseTransaction(const TxnHolder& txn_holder) {
  RemasterOccurredResult result;
  for (auto& lo_txn : txn_holder.lock_only_txns()) {
    if (lo_txn.has_value()) {
      ReleaseTransaction(lo_txn.value(), result);
    }
  }
  return result;
}

void PerKeyRemasterManager::ReleaseTransaction(const LockOnlyTxn& lo_txn, RemasterOccurredResult& result) {
  for (auto& key_info : lo_txn.keys) {
    if (blocked_queue_.count(key_info.key) > 0) {
      auto& queue = blocked_queue_[key_info.key];
      for (auto itr = queue.begin(); itr != queue.end(); itr++) {
        if ((*itr).first->holder.id() == lo_txn.holder.id()) {
          queue.erase(itr);
          break;  // Txns should only occur in a queue once
        }
      }
      if (queue.empty()) {
        blocked_queue_.erase(key_info.key);
      }
    }
  }

  for (auto& key_info : lo_txn.keys) {
    TryToUnblock(key_info.key, result);
  }
}

void PerKeyRemasterManager::InsertIntoBlockedQueue(const Key& key, const uint32_t counter, const LockOnlyTxn& lo_txn) {
  auto entry = make_pair(&lo_txn, counter);

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
  auto lock_only_txn = txn_pair.first;

  switch (CheckCounters(*lock_only_txn, storage_)) {
    case VerifyMasterResult::ABORT: {
      result.should_abort.push_back(lock_only_txn);
      ReleaseTransaction(*lock_only_txn, result);
      return;
    }
    case VerifyMasterResult::WAITING: {
      return;
    }
    case VerifyMasterResult::VALID: {
      for (auto& key_info : lock_only_txn->keys) {
        auto q_it = blocked_queue_.find(key_info.key);
        DCHECK(q_it != blocked_queue_.end()) << "Transaction was not in correct blocked_queues";
        auto& front_txn_replica_id = q_it->second.front().first;
        if (front_txn_replica_id != lock_only_txn) {
          return;
        }
      }
      result.unblocked.push_back(lock_only_txn);

      // Garbage collect queue and counters
      for (auto& key_info : lock_only_txn->keys) {
        auto q_it = blocked_queue_.find(key_info.key);
        q_it->second.pop_front();
        if (q_it->second.size() == 0) {
          blocked_queue_.erase(key_info.key);
        }
      }

      // Recurse on each updated queue
      for (auto& key_info : lock_only_txn->keys) {
        TryToUnblock(key_info.key, result);
      }
      break;
    }
  }
}

}  // namespace slog