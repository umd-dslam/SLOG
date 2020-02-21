#include "module/scheduler_components/deterministic_lock_manager.h"

#include <glog/logging.h>

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
      waiter_queue_.push_back(make_pair(txn_id, LockMode::READ));
      return false;
    default:
      return false;
  }
}

bool LockState::IsQueued(TxnId txn_id) {
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

DeterministicLockManager::DeterministicLockManager(ConfigurationPtr config, shared_ptr<Storage<Key, Record>> storage)
  : config_(config), storage_(storage) {}


bool DeterministicLockManager::RegisterTxn(const Transaction& txn) {
  auto txn_id = txn.internal().id();
  auto keys = ExtractKeys(txn);
  if (keys.empty()) {
    // None of the keys in this txn is in this partition
    return false;
  }

  num_locks_waited_[txn_id] += keys.size();
  if (num_locks_waited_[txn_id] == 0) {
    num_locks_waited_.erase(txn_id);
    return true;
  }
  return false;
}

bool DeterministicLockManager::AcquireLocks(const Transaction& txn) {
  auto txn_id = txn.internal().id();
  auto keys = ExtractKeys(txn);
  if (keys.empty()) {
    // None of the keys in this txn is in this partition
    return false;
  }

  for (auto pair : keys) {
    auto key = pair.first;
    auto mode = pair.second;
    switch (mode) {
      case LockMode::READ:
        if (!lock_table_[key].IsQueued(txn_id)
            && lock_table_[key].AcquireReadLock(txn_id)) {
          num_locks_waited_[txn_id]--;
        }
        break;
      case LockMode::WRITE:
        if (!lock_table_[key].IsQueued(txn_id)
            && lock_table_[key].AcquireWriteLock(txn_id)) {
          num_locks_waited_[txn_id]--;
        }
        break;
      default:
        LOG(FATAL) << "Invalid lock mode";
        break;
    }
  }

  if (num_locks_waited_[txn_id] == 0) {
    num_locks_waited_.erase(txn_id);
    return true;
  }
  return false;
}

bool DeterministicLockManager::RegisterTxnAndAcquireLocks(const Transaction& txn) {
  RegisterTxn(txn);
  return AcquireLocks(txn);
}

unordered_set<TxnId>
DeterministicLockManager::ReleaseLocks(const Transaction& txn) {
  unordered_set<TxnId> ready_txns;
  auto txn_id = txn.internal().id();
  auto keys = ExtractKeys(txn);

  for (const auto& pair : keys) {
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

  num_locks_waited_.erase(txn_id);

  return ready_txns;
}

VerifyMasterResult
DeterministicLockManager::VerifyMaster(const Transaction& txn) {
  auto keys = ExtractKeys(txn);
  if (keys.empty()) {
    // None of the keys in this txn is in this partition
    return VerifyMasterResult::Valid;
  }

  auto txn_master_metadata = txn.internal().master_metadata();

  std::unordered_map<Key, int32_t> ahead_counters;
  for (auto pair : keys) {
    auto key = pair.first;
    CHECK(txn_master_metadata.contains(key))
              << "Master metadata for key \"" << key << "\" is missing";

    Record record;
    bool found = storage_->Read(key, record);
    if (found) {
      auto txn_metadata = txn_master_metadata.at(key);
      auto stored_metadata = record.metadata;

      if (txn_metadata.counter() > stored_metadata.counter) {
        ahead_counters[key] += txn_metadata.counter() - stored_metadata.counter;
      } else if (txn_metadata.counter() < stored_metadata.counter) {
        return VerifyMasterResult::Aborted;
      } else {
        CHECK(txn_metadata.master() == stored_metadata.master)
                << "Metadata is invalid, different masters with the same counter";
      }
    } else { // key not in storage
      // TODO: verify if counter is 0?
    }
  }
  if (ahead_counters.size()){
    auto waiting_remaster_entry = make_pair(txn.internal().id(), ahead_counters);
    for (auto key : ahead_counters) {
      keys_waiting_remaster_[key.first].insert(&waiting_remaster_entry);
    }
    return VerifyMasterResult::Waiting;
  } else {
    return VerifyMasterResult::Valid;
  }
}

unordered_set<TxnId> DeterministicLockManager::RemasterOccured(unordered_set<Key> keys) {
  unordered_set<TxnId> unblocked;

  for (auto key : keys) {
    for (auto entry : keys_waiting_remaster_[key]) {
      auto txn_id = entry->first;
      auto waiting_map = entry->second;

      waiting_map[key] -= 1;
      if (waiting_map[key] == 0) {
        waiting_map.erase(key);
      }
      if (waiting_map.empty()) {
        unblocked.insert(txn_id);
        keys_waiting_remaster_[key].erase(entry);
      }
      if (keys_waiting_remaster_[key].empty()) {
        keys_waiting_remaster_.erase(key);
      }
    }
  }
  return unblocked;
}

vector<pair<Key, LockMode>>
DeterministicLockManager::ExtractKeys(const Transaction& txn) {
  vector<pair<Key, LockMode>> keys;
  for (const auto& kv : txn.read_set()) {
    // If this key is also in write_set, give it write lock instead
    if (config_->KeyIsInLocalPartition(kv.first) 
        && !txn.write_set().contains(kv.first)) {
      keys.emplace_back(kv.first, LockMode::READ);
    }
  }
  for (const auto& kv : txn.write_set()) {
    if (config_->KeyIsInLocalPartition(kv.first)) {
      keys.emplace_back(kv.first, LockMode::WRITE);
    }
  }
  return keys;
}


} // namespace slog