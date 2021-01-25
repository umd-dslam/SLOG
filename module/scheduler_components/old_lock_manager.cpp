#include "module/scheduler_components/old_lock_manager.h"

#include <glog/logging.h>

#include <algorithm>

using std::make_pair;
using std::move;

namespace slog {

bool OldLockState::AcquireReadLock(TxnId txn_id) {
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

bool OldLockState::AcquireWriteLock(TxnId txn_id) {
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

bool OldLockState::Contains(TxnId txn_id) { return holders_.count(txn_id) > 0 || waiters_.count(txn_id) > 0; }

unordered_set<TxnId> OldLockState::Release(TxnId txn_id) {
  // If the transaction is not among the lock holders, find and remove it in
  // the queue of waiters
  if (holders_.count(txn_id) == 0) {
    waiter_queue_.erase(std::remove_if(waiter_queue_.begin(), waiter_queue_.end(),
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

AcquireLocksResult OldLockManager::AcquireLocks(const LockOnlyTxn& lo_txn) {
  auto txn_id = lo_txn.holder.id();

  auto ins = num_locks_waited_.try_emplace(txn_id, lo_txn.holder.keys_in_partition().size());

  int num_locks_acquired = 0;
  for (auto key_info : lo_txn.keys) {
    auto& key = key_info.key;
    auto mode = key_info.mode;
    auto& lock_state = lock_table_[key];
    DCHECK(!lock_state.Contains(txn_id)) << "Txn requested lock twice: " << txn_id << ", " << key;
    auto before_mode = lock_state.mode;
    switch (mode) {
      case LockMode::READ:
        if (lock_state.AcquireReadLock(txn_id)) {
          num_locks_acquired++;
        }
        break;
      case LockMode::WRITE:
        if (lock_state.AcquireWriteLock(txn_id)) {
          num_locks_acquired++;
        }
        break;
      default:
        LOG(FATAL) << "Invalid lock mode";
        break;
    }
    if (before_mode == LockMode::UNLOCKED && lock_state.mode != before_mode) {
      num_locked_keys_++;
    }
  }

  auto num_locked_waited_it = ins.first;
  num_locked_waited_it->second -= num_locks_acquired;
  if (num_locked_waited_it->second == 0) {
    num_locks_waited_.erase(num_locked_waited_it);
    return AcquireLocksResult::ACQUIRED;
  }
  return AcquireLocksResult::WAITING;
}

vector<TxnId> OldLockManager::ReleaseLocks(const TxnHolder& txn_holder) {
  vector<TxnId> result;
  auto txn_id = txn_holder.id();

  for (const auto& key : txn_holder.keys_in_partition()) {
    auto lock_state_it = lock_table_.find(key.first);
    if (lock_state_it == lock_table_.end()) {
      continue;
    }
    auto& lock_state = lock_state_it->second;
    auto old_mode = lock_state.mode;
    auto new_grantees = lock_state.Release(txn_id);
    // Prevent the lock table from growing too big
    if (lock_state.mode == LockMode::UNLOCKED) {
      if (old_mode != LockMode::UNLOCKED) {
        num_locked_keys_--;
      }
      if (lock_table_.size() > kLockTableSizeLimit) {
        lock_table_.erase(key.first);
      }
    }

    for (auto new_txn : new_grantees) {
      auto it = num_locks_waited_.find(new_txn);
      DCHECK(it != num_locks_waited_.end());
      it->second--;
      if (it->second == 0) {
        num_locks_waited_.erase(it);
        result.push_back(new_txn);
      }
    }
  }

  // Deduplicate the result
  std::sort(result.begin(), result.end());
  auto last = std::unique(result.begin(), result.end());
  result.erase(last, result.end());

  num_locks_waited_.erase(txn_id);

  return result;
}

void OldLockManager::GetStats(rapidjson::Document& stats, uint32_t level) const {
  using rapidjson::StringRef;

  auto& alloc = stats.GetAllocator();
  stats.AddMember(StringRef(NUM_TXNS_WAITING_FOR_LOCK), num_locks_waited_.size(), alloc);

  if (level >= 1) {
    // Collect number of locks waited per txn
    stats.AddMember(StringRef(NUM_LOCKS_WAITED_PER_TXN), ToJsonArrayOfKeyValue(num_locks_waited_, alloc), alloc);
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
          .PushBack(ToJsonArray(lock_state.GetHolders(), alloc), alloc)
          .PushBack(ToJsonArrayOfKeyValue(
                        lock_state.GetWaiters(), [](const auto& v) { return static_cast<uint32_t>(v); }, alloc),
                    alloc);
      lock_table.PushBack(move(entry), alloc);
    }
    stats.AddMember(StringRef(LOCK_TABLE), move(lock_table), alloc);
  }
}

}  // namespace slog