#include "module/scheduler_components/per_key_remaster_manager.h"

#include <glog/logging.h>

using std::make_pair;

namespace slog {

PerKeyRemasterManager::PerKeyRemasterManager(const shared_ptr<const Storage>& storage) : storage_(storage) {}

VerifyMasterResult PerKeyRemasterManager::VerifyMaster(Transaction& txn) {
  if (txn.keys().empty()) {
    // None of the keys in this txn are in this partition
    return VerifyMasterResult::VALID;
  }

  switch (CheckCounters(txn, true, storage_)) {
    case VerifyMasterResult::ABORT: {
      return VerifyMasterResult::ABORT;
    }
    case VerifyMasterResult::VALID: {
      // Check if other transactions are blocking the queue
      auto indirectly_blocked = false;
      for (const auto& kv : txn.keys()) {
        auto q_it = blocked_queue_.find(kv.key());
        // If the queue is emtpy, can be skipped
        if (q_it != blocked_queue_.end() && !q_it->second.empty()) {
          // If we won't be at the front of the queue, the txn will need to wait.
          if (q_it->second.front().second <= kv.value_entry().metadata().counter()) {
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
  for (const auto& kv : txn.keys()) {
    const auto& key = kv.key();
    const auto& value = kv.value_entry();
    if (static_cast<int>(value.metadata().master()) == txn.internal().home()) {
      InsertIntoBlockedQueue(key, value.metadata().counter(), txn);
    }
  }
  return VerifyMasterResult::WAITING;
}

RemasterOccurredResult PerKeyRemasterManager::ReleaseTransaction(const Transaction& txn) {
  for (const auto& kv : txn.keys()) {
    if (blocked_queue_.count(kv.key()) > 0) {
      auto& queue = blocked_queue_[kv.key()];
      for (auto itr = queue.begin(); itr != queue.end(); itr++) {
        if ((*itr).first->internal().id() == txn.internal().id()) {
          queue.erase(itr);
          break;  // Txns should only occur in a queue once
        }
      }
      if (queue.empty()) {
        blocked_queue_.erase(kv.key());
      }
    }
  }

  RemasterOccurredResult result;
  for (const auto& kv : txn.keys()) {
    TryToUnblock(kv.key(), result);
  }
  return result;
}

void PerKeyRemasterManager::InsertIntoBlockedQueue(const Key& key, const uint32_t counter, Transaction& txn) {
  auto entry = make_pair(&txn, counter);

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

  switch (CheckCounters(*lock_only_txn, true, storage_)) {
    case VerifyMasterResult::ABORT: {
      result.should_abort.push_back(lock_only_txn);
      auto res = ReleaseTransaction(*lock_only_txn);
      result.should_abort.splice(result.should_abort.end(), res.should_abort);
      result.unblocked.splice(result.unblocked.end(), res.unblocked);
      return;
    }
    case VerifyMasterResult::WAITING: {
      return;
    }
    case VerifyMasterResult::VALID: {
      for (const auto& kv : lock_only_txn->keys()) {
        if (static_cast<int>(kv.value_entry().metadata().master()) == lock_only_txn->internal().home()) {
          auto q_it = blocked_queue_.find(kv.key());
          DCHECK(q_it != blocked_queue_.end()) << "Transaction was not in correct blocked_queues";
          auto& front_txn_replica_id = q_it->second.front().first;
          if (front_txn_replica_id != lock_only_txn) {
            return;
          }
        }
      }
      result.unblocked.push_back(lock_only_txn);

      // Garbage collect queue and counters
      for (const auto& kv : lock_only_txn->keys()) {
        if (static_cast<int>(kv.value_entry().metadata().master()) == lock_only_txn->internal().home()) {
          auto q_it = blocked_queue_.find(kv.key());
          q_it->second.pop_front();
          if (q_it->second.size() == 0) {
            blocked_queue_.erase(kv.key());
          }
        }
      }

      // Recurse on each updated queue
      for (const auto& kv : lock_only_txn->keys()) {
        if (static_cast<int>(kv.value_entry().metadata().master()) == lock_only_txn->internal().home()) {
          TryToUnblock(kv.key(), result);
        }
      }
      break;
    }
  }
}

}  // namespace slog