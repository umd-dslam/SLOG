#include "module/scheduler_components/deterministic_lock_manager.h"

using std::make_pair;

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
        waiters_.push_back(make_pair(txn_id, LockMode::READ));
        return false;
      }
    case LockMode::WRITE:
      waiters_.push_back(make_pair(txn_id, LockMode::READ));
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
      waiters_.push_back(make_pair(txn_id, LockMode::READ));
      return false;
    default:
      return false;
  }
}

unordered_set<TxnId> LockState::Release(TxnId txn_id) {
  // If the transaction is not among the lock holders, find and remove it in
  // the queue of waiters
  if (holders_.count(txn_id) == 0) {
    auto it = std::find_if(
        waiters_.begin(),
        waiters_.end(),
        [txn_id](auto pair) { return pair.first == txn_id; });
    if (it != waiters_.end()) {
      waiters_.erase(it);
    }
    // No new transaction get the lock
    return {};
  }


  holders_.erase(txn_id);

  // If there are still holders of this lock, do nothing
  if (!holders_.empty()) {
    // No new transaction get the lock
    return {};
  }

  // If all holders release the lock but no waiters, the current state is
  // changed to unlocked
  if (waiters_.empty()) {
    mode = LockMode::UNLOCKED;
    // No new transaction get the lock
    return {};
  }

  auto front = waiters_.front();
  if (front.second == LockMode::READ) {
    // Gives the READ lock to all read transactions at the head of the queue
    do {
      holders_.insert(waiters_.front().first);
      waiters_.pop_front();
    } while (!waiters_.empty() && waiters_.front().second == LockMode::READ);

    mode = LockMode::READ;

  } else if (front.second == LockMode::WRITE) {
    // Give the WRITE lock to a single transaction at the head of the queue
    holders_.insert(front.first);
    waiters_.pop_front();
    mode = LockMode::WRITE;
  }
  return holders_;
}

DeterministicLockManager::DeterministicLockManager(
    shared_ptr<Configuration> config)
  : config_(config) {}

bool DeterministicLockManager::AcquireLocks(const Transaction& txn) {
  auto txn_id = txn.internal().id();
  num_locks_waited_[txn_id] = 0;
  for (const auto& pair : txn.read_set()) {
    auto key = pair.first;
    // Ignore this key if it is not in the current partition
    if (!config_->KeyIsInLocalPartition(key)) {
      continue;
    }
    // If this key is also in the writeset, give it write lock instead
    if (txn.write_set().contains(key)) {
      continue;
    }
    if (!lock_table_[key].AcquireReadLock(txn_id)) {
      num_locks_waited_[txn_id]++;
    }
  }
  for (const auto& pair : txn.write_set()) {
    auto key = pair.first;
    // Ignore this key if it is not in the current partition
    if (!config_->KeyIsInLocalPartition(key)) {
      continue;
    }
    if (!lock_table_[key].AcquireWriteLock(txn_id)) {
      num_locks_waited_[txn_id]++;
    }
  }
  if (num_locks_waited_[txn_id] == 0) {
    num_locks_waited_.erase(txn_id);
    return true;
  }
  return false;
}

unordered_set<TxnId>
DeterministicLockManager::ReleaseLocks(const Transaction& txn) {
  unordered_set<TxnId> ready_txns;
  auto txn_id = txn.internal().id();
  auto Release = 
      [this, txn_id, &ready_txns]
      (const auto& read_or_write_set) {
    for (const auto& pair : read_or_write_set) {
      auto key = pair.first;
      if (!config_->KeyIsInLocalPartition(key)) {
        continue;
      }

      auto new_holders = lock_table_[key].Release(txn_id);
      for (auto holder : new_holders) {
        num_locks_waited_[holder]--;
        if (num_locks_waited_[holder] == 0) {
          num_locks_waited_.erase(holder);
          ready_txns.insert(holder);
        }
      }

      // Prevent the lock table from growing too big
      if (lock_table_[key].mode == LockMode::UNLOCKED && 
          lock_table_.size() > LOCK_TABLE_SIZE_LIMIT) {
        lock_table_.erase(key);
      }
    }
  };

  Release(txn.read_set());
  Release(txn.write_set());

  return ready_txns;
}

} // namespace slog