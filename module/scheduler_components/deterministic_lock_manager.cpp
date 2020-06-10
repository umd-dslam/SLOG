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

DeterministicLockManager::DeterministicLockManager(const std::shared_ptr<const Storage<Key, Record>>& storage)
    : storage_(storage) {}

bool DeterministicLockManager::AcceptTransaction(const TransactionHolder& txn_holder) {
  if (txn_holder.KeysInPartition().empty()) {
    LOG(FATAL) << "Empty txn should not have reached lock manager";
    return false;
  }

  auto txn = txn_holder.GetTransaction();
  auto txn_id = txn->internal().id();
  if (txn->procedure_case() == Transaction::kRemaster) {
    num_locks_waited_[txn_id] = 2;
  } else {
    num_locks_waited_[txn_id] += txn_holder.KeysInPartition().size();
  }

  if (num_locks_waited_[txn_id] == 0) {
    num_locks_waited_.erase(txn_id);
    return true;
  }
  return false;
}

AcquireLocksResult DeterministicLockManager::AcquireLocks(const TransactionHolder& txn_holder) {
  if (txn_holder.KeysInPartition().empty()) {
    LOG(FATAL) << "Empty txn should not have reached lock manager";
    return AcquireLocksResult::WAITING;
  }

  auto txn_id = txn_holder.GetTransaction()->internal().id();
  auto txn = txn_holder.GetTransaction();

  vector<pair<KeyReplica, LockMode>> locks_to_request;
  if (txn->procedure_case() == Transaction::kRemaster) {
    auto pair = *txn_holder.KeysInPartition().begin();
    auto key = pair.first;
    auto mode = LockMode::WRITE;
    auto master = txn->internal().master_metadata().at(key).master();
    if (txn->remaster().new_master_lock_only()) {
      master = txn->remaster().new_master();
    }
    auto key_replica = MakeKeyReplica(key, master);
    locks_to_request.push_back(make_pair(key_replica, mode));
  } else {
    for (auto pair : txn_holder.KeysInPartition()) {
      auto key = pair.first;
      auto mode = pair.second;
      auto master = txn->internal().master_metadata().at(key).master();
      auto key_replica = MakeKeyReplica(key, master);
      locks_to_request.push_back(make_pair(key_replica, mode));
    }
  }

  int num_locks_acquired = 0;
  for (auto pair : locks_to_request) {
    auto key_replica = pair.first;
    auto mode = pair.second;

    CHECK(!lock_table_[key_replica].Contains(txn_id))
        << "Txn requested lock twice: " << txn_id << ", " << key_replica;
    auto before_mode = lock_table_[key_replica].mode;
    switch (mode) {
      case LockMode::READ:
        if (lock_table_[key_replica].AcquireReadLock(txn_id)) {
          num_locks_acquired++;
        }
        break;
      case LockMode::WRITE:
        if (lock_table_[key_replica].AcquireWriteLock(txn_id)) {
          num_locks_acquired++;
        }
        break;
      default:
        LOG(FATAL) << "Invalid lock mode";
        break;
    }
    if (before_mode == LockMode::UNLOCKED && lock_table_[key_replica].mode != before_mode) {
      num_locked_keys_++;
    }
  }

  if (num_locks_acquired > 0) {
    num_locks_waited_[txn_id] -= num_locks_acquired;
    if (num_locks_waited_[txn_id] == 0) {
      num_locks_waited_.erase(txn_id);
      return AcquireLocksResult::ACQUIRED;
    }
  }
  return AcquireLocksResult::WAITING;
}

AcquireLocksResult DeterministicLockManager::AcceptTransactionAndAcquireLocks(const TransactionHolder& txn_holder) {
  AcceptTransaction(txn_holder);
  return AcquireLocks(txn_holder);
}

unordered_set<TxnId> DeterministicLockManager::ReleaseLocks(const TransactionHolder& txn_holder) {
  unordered_set<TxnId> result;
  auto txn = txn_holder.GetTransaction();
  auto txn_id = txn->internal().id();

  vector<KeyReplica> locks_to_release;
  if (txn->procedure_case() == Transaction::kRemaster) {
    // TODO: old lock can be deleted. Waiting txns are aborted, unless they are a remaster
    auto pair = *txn_holder.KeysInPartition().begin();
    auto key = pair.first;
    auto old_master = txn->internal().master_metadata().at(key).master();
    auto old_key_replica = MakeKeyReplica(key, old_master);
    locks_to_release.push_back(old_key_replica);
    auto new_master = txn->remaster().new_master();
    auto new_key_replica = MakeKeyReplica(key, new_master);
    locks_to_release.push_back(new_key_replica);
  } else {
    for (auto pair : txn_holder.KeysInPartition()) {
      auto key = pair.first;
      auto master = txn->internal().master_metadata().at(key).master();
      auto key_replica = MakeKeyReplica(key, master);
      locks_to_release.push_back(key_replica);
    }
  }

  for (auto key_replica : locks_to_release) {
    auto old_mode = lock_table_[key_replica].mode;
    auto new_grantees = lock_table_[key_replica].Release(txn_id);
     // Prevent the lock table from growing too big
     // TODO: automatically delete remastered keys
    if (lock_table_[key_replica].mode == LockMode::UNLOCKED) {
      if (old_mode != LockMode::UNLOCKED) {
        num_locked_keys_--;
      }
      if (lock_table_.size() > LOCK_TABLE_SIZE_LIMIT) {
        lock_table_.erase(key_replica);
      }
    }

    for (auto new_txn : new_grantees) {
      num_locks_waited_[new_txn]--;
      if (num_locks_waited_[new_txn] == 0) {
        num_locks_waited_.erase(new_txn);
        result.insert(new_txn);
      }
    }
  }

  num_locks_waited_.erase(txn_id);

  return result;
}

KeyReplica DeterministicLockManager::MakeKeyReplica(Key key, uint32_t master) {
  // Note: this is unique, since keys cannot contain spaces
  return key + " " + std::to_string(master);
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