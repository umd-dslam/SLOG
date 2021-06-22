#include "module/scheduler_components/rma_lock_manager.h"

#include <glog/logging.h>

#include <algorithm>

using std::make_pair;
using std::move;

namespace slog {

bool LockState::AcquireReadLock(TxnId txn_id) {
  switch (mode) {
    case LockMode::UNLOCKED:
      holders_.push_back(txn_id);
      mode = LockMode::READ;
      return true;
    case LockMode::READ:
      if (waiter_queue_.empty()) {
        holders_.push_back(txn_id);
        return true;
      } else {
        waiter_queue_.push_back(make_pair(txn_id, LockMode::READ));
        return false;
      }
    case LockMode::WRITE:
      waiter_queue_.push_back(make_pair(txn_id, LockMode::READ));
      return false;
    default:
      return false;
  }
}

bool LockState::AcquireWriteLock(TxnId txn_id) {
  switch (mode) {
    case LockMode::UNLOCKED:
      holders_.push_back(txn_id);
      mode = LockMode::WRITE;
      return true;
    case LockMode::READ:
    case LockMode::WRITE:
      waiter_queue_.push_back(make_pair(txn_id, LockMode::WRITE));
      return false;
    default:
      return false;
  }
}

bool LockState::Contains(TxnId txn_id) {
  return std::find(holders_.begin(), holders_.end(), txn_id) != holders_.end() ||
         std::find_if(waiter_queue_.begin(), waiter_queue_.end(),
                      [txn_id](auto& pair) { return pair.first == txn_id; }) != waiter_queue_.end();
}

vector<TxnId> LockState::Release(TxnId txn_id) {
  // If the transaction is not among the lock holders, find and remove it in
  // the queue of waiters
  auto it = std::find(holders_.begin(), holders_.end(), txn_id);
  if (it == holders_.end()) {
    waiter_queue_.erase(std::remove_if(waiter_queue_.begin(), waiter_queue_.end(),
                                       [txn_id](auto& pair) { return pair.first == txn_id; }),
                        waiter_queue_.end());
    // No new transaction get the lock
    return {};
  }

  holders_.erase(it);

  // If there are still holders for this lock, do nothing
  if (!holders_.empty()) {
    // No new transaction gets the lock
    return {};
  }

  // If all holders release the lock but there is no waiter, the current state is
  // changed to unlocked
  if (waiter_queue_.empty()) {
    mode = LockMode::UNLOCKED;
    // No new transaction get the lock
    return {};
  }

  auto front = waiter_queue_.front();
  if (front.second == LockMode::READ) {
    // Gives the READ lock to all read transactions at the head of the queue
    do {
      auto txn_id = waiter_queue_.front().first;
      holders_.push_back(txn_id);
      waiter_queue_.pop_front();
    } while (!waiter_queue_.empty() && waiter_queue_.front().second == LockMode::READ);

    mode = LockMode::READ;

  } else if (front.second == LockMode::WRITE) {
    // Give the WRITE lock to a single transaction at the head of the queue
    holders_.push_back(front.first);
    waiter_queue_.pop_front();
    mode = LockMode::WRITE;
  }
  return holders_;
}

AcquireLocksResult RMALockManager::AcquireLocks(const Transaction& txn) {
  auto txn_id = txn.internal().id();
  auto home = txn.internal().home();
  auto is_remaster = txn.program_case() == Transaction::kRemaster;

  // A remaster txn only has one key K but it acquires locks on (K, RO) and (K, RN)
  // where RO and RN are the old and new region respectively.
  auto num_required_locks = is_remaster ? 2 : txn.keys_size();
  auto ins = txn_info_.try_emplace(txn_id, num_required_locks);
  auto& txn_info = ins.first->second;

  for (const auto& kv : txn.keys()) {
    // Skip keys that does not belong to the assigned home. Remaster txn is an exception where
    // it is allowed that the metadata on the txn does not match its assigned home
    if (!is_remaster && static_cast<int>(kv.value_entry().metadata().master()) != home) {
      continue;
    }

    auto key_replica = MakeKeyReplica(kv.key(), home);
    txn_info.keys.push_back(key_replica);

    auto& lock_state = lock_table_[key_replica];

    DCHECK(!lock_state.Contains(txn_id)) << "Txn requested lock twice: " << txn_id << ", " << key_replica;

    auto before_mode = lock_state.mode;
    switch (kv.value_entry().type()) {
      case KeyType::READ:
        if (lock_state.AcquireReadLock(txn_id)) {
          txn_info.num_waiting_for--;
        }
        break;
      case KeyType::WRITE:
        if (lock_state.AcquireWriteLock(txn_id)) {
          txn_info.num_waiting_for--;
        }
        break;
      default:
        LOG(FATAL) << "Invalid lock mode";
    }
    if (before_mode == LockMode::UNLOCKED && lock_state.mode != before_mode) {
      num_locked_keys_++;
    }
  }

  if (txn_info.is_ready()) {
    return AcquireLocksResult::ACQUIRED;
  }
  return AcquireLocksResult::WAITING;
}

vector<TxnId> RMALockManager::ReleaseLocks(TxnId txn_id) {
  vector<TxnId> result;
  auto info_it = txn_info_.find(txn_id);
  if (info_it == txn_info_.end()) {
    return result;
  }
  auto& info = info_it->second;
  for (const auto& key_replica : info.keys) {
    auto lock_state_it = lock_table_.find(key_replica);
    if (lock_state_it == lock_table_.end()) {
      continue;
    }
    auto& lock_state = lock_state_it->second;
    auto old_mode = lock_state.mode;
    auto new_grantees = lock_state.Release(txn_id);
    // Prevent the lock table from growing too big
    // TODO: automatically delete remastered keys
    if (lock_state.mode == LockMode::UNLOCKED) {
      if (old_mode != LockMode::UNLOCKED) {
        num_locked_keys_--;
      }
    }

    for (auto new_txn : new_grantees) {
      auto it = txn_info_.find(new_txn);
      DCHECK(it != txn_info_.end());
      it->second.num_waiting_for--;
      if (it->second.is_ready()) {
        result.push_back(new_txn);
      }
    }
  }

  txn_info_.erase(info_it);

  // Deduplicate the result
  std::sort(result.begin(), result.end());
  auto last = std::unique(result.begin(), result.end());
  result.erase(last, result.end());

  return result;
}

/**
 * {
 *    lock_manager_type: 0,
 *    num_txns_waiting_for_lock: <int>,
 *    num_waiting_for_per_txn (lvl >= 1): [
 *      [<txn id>, <number of locks waited>],
 *      ...
 *    ],
 *    num_locked_keys: <number of keys locked>,
 *    lock_table (lvl >= 2): [
 *      [
 *        <key>,
 *        <mode>,
 *        [<holder>, ...],
 *        [[<waiting txn id>, <mode>], ...]
 *      ],
 *      ...
 *    ],
 * }
 */
void RMALockManager::GetStats(rapidjson::Document& stats, uint32_t level) const {
  using rapidjson::StringRef;

  auto& alloc = stats.GetAllocator();
  stats.AddMember(StringRef(LOCK_MANAGER_TYPE), 0, alloc);
  stats.AddMember(StringRef(NUM_TXNS_WAITING_FOR_LOCK), txn_info_.size(), alloc);

  if (level >= 1) {
    // Collect number of locks waited per txn
    stats.AddMember(StringRef(NUM_WAITING_FOR_PER_TXN),
                    ToJsonArrayOfKeyValue(
                        txn_info_, [](const auto& info) { return info.num_waiting_for; }, alloc),
                    alloc);
  }

  stats.AddMember(StringRef(NUM_LOCKED_KEYS), num_locked_keys_, alloc);
  if (level >= 2) {
    // Collect data from lock tables
    rapidjson::Value lock_table(rapidjson::kArrayType);
    for (const auto& [key, lock_state] : lock_table_) {
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