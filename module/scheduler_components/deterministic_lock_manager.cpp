#include "module/scheduler_components/deterministic_lock_manager.h"

#include <glog/logging.h>

using std::make_pair;
using std::move;

namespace slog {

bool LockState::AcquireReadLock(TxnId txn_id) {
  switch (mode) {
    case LockMode::UNLOCKED:
      holders_.insert(txn_id);
      mode = LockMode::READ;
      return true;
    case LockMode::READ:
      if (waiters_.empty()) {
        holders_.insert(txn_id);
        return true;
      } else {
        waiters_.insert(txn_id);
        waiter_queue_.push_back(make_pair(txn_id, LockMode::READ));
        return false;
      }
    case LockMode::WRITE:
      waiters_.insert(txn_id);
      waiter_queue_.push_back(make_pair(txn_id, LockMode::READ));
      return false;
    default:
      return false;
  }
}

bool LockState::AcquireWriteLock(TxnId txn_id) {
  switch (mode) {
    case LockMode::UNLOCKED:
      holders_.insert(txn_id);
      mode = LockMode::WRITE;
      return true;
    case LockMode::READ:
    case LockMode::WRITE:
      waiters_.insert(txn_id);
      waiter_queue_.push_back(make_pair(txn_id, LockMode::WRITE));
      return false;
    default:
      return false;
  }
}

bool LockState::Contains(TxnId txn_id) {
  return holders_.count(txn_id) > 0 || waiters_.count(txn_id) > 0;
}

unordered_set<TxnId> LockState::Release(TxnId txn_id) {
  // If the transaction is not among the lock holders, find and remove it in
  // the queue of waiters
  if (holders_.count(txn_id) == 0) {
    waiter_queue_.erase(
      std::remove_if(
          waiter_queue_.begin(),
          waiter_queue_.end(),
          [txn_id](auto& pair) { return pair.first == txn_id; }),
      waiter_queue_.end());
    waiters_.erase(txn_id);
    // No new transaction get the lock
    return {};
  }

  holders_.erase(txn_id);

  // If there are still holders for this lock, do nothing
  if (!holders_.empty()) {
    // No new transaction get the lock
    return {};
  }

  // If all holders release the lock but there is no waiter, the current state is
  // changed to unlocked
  if (waiters_.empty()) {
    mode = LockMode::UNLOCKED;
    // No new transaction get the lock
    return {};
  }

  auto front = waiter_queue_.front();
  if (front.second == LockMode::READ) {
    // Gives the READ lock to all read transactions at the head of the queue
    do {
      auto txn_id = waiter_queue_.front().first;
      holders_.insert(txn_id);
      waiters_.erase(txn_id);
      waiter_queue_.pop_front();
    } while (!waiter_queue_.empty() && waiter_queue_.front().second == LockMode::READ);

    mode = LockMode::READ;

  } else if (front.second == LockMode::WRITE) {
    // Give the WRITE lock to a single transaction at the head of the queue
    holders_.insert(front.first);
    waiters_.erase(front.first);
    waiter_queue_.pop_front();
    mode = LockMode::WRITE;
  }
  return holders_;
}

bool DeterministicLockManager::AcceptTransaction(const TransactionHolder& txn_holder) {
  if (txn_holder.KeysInPartition().empty()) {
    return false;
  }
  auto txn_id = txn_holder.GetTransaction()->internal().id();
  num_locks_waited_[txn_id] += txn_holder.KeysInPartition().size();
  if (num_locks_waited_[txn_id] == 0) {
    num_locks_waited_.erase(txn_id);
    return true;
  }
  return false;
}

bool DeterministicLockManager::AcquireLocks(const TransactionHolder& txn_holder) {
  if (txn_holder.KeysInPartition().empty()) {
    return false;
  }
  auto txn_id = txn_holder.GetTransaction()->internal().id();
  int num_locks_acquired = 0;
  for (auto pair : txn_holder.KeysInPartition()) {
    auto key = pair.first;
    auto mode = pair.second;
    if (!lock_table_[key].Contains(txn_id)) {
      auto before_mode = lock_table_[key].mode;
      switch (mode) {
        case LockMode::READ:
          if (lock_table_[key].AcquireReadLock(txn_id)) {
            num_locks_acquired++;
          }
          break;
        case LockMode::WRITE:
          if (lock_table_[key].AcquireWriteLock(txn_id)) {
            num_locks_acquired++;
          }
          break;
        default:
          LOG(FATAL) << "Invalid lock mode";
          break;
      }
      if (before_mode == LockMode::UNLOCKED && lock_table_[key].mode != before_mode) {
        num_locked_keys_++;
      }
    }
  }

  if (num_locks_acquired > 0) {
    num_locks_waited_[txn_id] -= num_locks_acquired;
    if (num_locks_waited_[txn_id] == 0) {
      num_locks_waited_.erase(txn_id);
      return true;
    }
  }
  return false;
}

bool DeterministicLockManager::AcceptTransactionAndAcquireLocks(const TransactionHolder& txn_holder) {
  AcceptTransaction(txn_holder);
  return AcquireLocks(txn_holder);
}

unordered_set<TxnId>
DeterministicLockManager::ReleaseLocks(const TransactionHolder& txn_holder) {
  unordered_set<TxnId> ready_txns;
  auto txn_id = txn_holder.GetTransaction()->internal().id();

  for (const auto& pair : txn_holder.KeysInPartition()) {
    auto key = pair.first;
    auto old_mode = lock_table_[key].mode;
    auto new_grantees = lock_table_[key].Release(txn_id);
     // Prevent the lock table from growing too big
    if (lock_table_[key].mode == LockMode::UNLOCKED) {
      if (old_mode != LockMode::UNLOCKED) {
        num_locked_keys_--;
      }
      if (lock_table_.size() > LOCK_TABLE_SIZE_LIMIT) {
        lock_table_.erase(key);
      }
    }

    for (auto new_txn : new_grantees) {
      num_locks_waited_[new_txn]--;
      if (num_locks_waited_[new_txn] == 0) {
        num_locks_waited_.erase(new_txn);
        ready_txns.insert(new_txn);
      }
    }
  }

  num_locks_waited_.erase(txn_id);

  return ready_txns;
}

void DeterministicLockManager::GetStats(rapidjson::Document& stats, uint32_t level) const {
  using rapidjson::StringRef;

  auto& alloc = stats.GetAllocator();
  stats.AddMember(StringRef(NUM_TXNS_WAITING_FOR_LOCK), num_locks_waited_.size(), alloc);

  if (level >= 1) {
    // Collect number of locks waited per txn
    stats.AddMember(
        StringRef(NUM_LOCKS_WAITED_PER_TXN),
        ToJsonArrayOfKeyValue(num_locks_waited_, alloc),
        alloc);
  }

  stats.AddMember(StringRef(NUM_LOCKED_KEYS), num_locked_keys_, alloc);
  if (level >= 2) {
    // Collect data from lock tables
    rapidjson::Value lock_table(rapidjson::kArrayType);
    for (const auto& pair : lock_table_) {
      auto& key = pair.first;
      auto& lock_state = pair.second;
      if (lock_state.mode == LockMode::UNLOCKED) {
        continue;
      }
      rapidjson::Value entry(rapidjson::kArrayType);
      rapidjson::Value key_json(key.c_str(), alloc);
      entry.PushBack(key_json, alloc)
           .PushBack(static_cast<uint32_t>(lock_state.mode), alloc)
           .PushBack(
                ToJsonArray(lock_state.GetHolders(), alloc), alloc)
           .PushBack(
                ToJsonArrayOfKeyValue(
                    lock_state.GetWaiters(),
                    [](const auto& v) { return static_cast<uint32_t>(v); },
                    alloc),
                alloc);
      lock_table.PushBack(move(entry), alloc);
    }
    stats.AddMember(StringRef(LOCK_TABLE), move(lock_table), alloc);
  }
}

} // namespace slog