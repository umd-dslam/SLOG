#include "module/scheduler_components/ddr_lock_manager.h"

#include <glog/logging.h>

using std::make_pair;
using std::move;

namespace slog {

optional<TxnId> LockQueueTail::AcquireReadLock(TxnId txn_id) {
  read_lock_requesters_.push_back(txn_id);
  return write_lock_requester_;
}

vector<TxnId> LockQueueTail::AcquireWriteLock(TxnId txn_id) {
  vector<TxnId> deps;
  if (read_lock_requesters_.empty()) {
    if (write_lock_requester_.has_value()) {
      deps.push_back(write_lock_requester_.value());
    }
  } else {
    deps.insert(deps.end(), read_lock_requesters_.begin(), read_lock_requesters_.end());
    read_lock_requesters_.clear();
  }
  write_lock_requester_ = txn_id;
  return deps;
}

bool DDRLockManager::AcceptTransaction(const TxnHolder& txn_holder) {
  if (txn_holder.keys_in_partition().empty()) {
    LOG(FATAL) << "Empty txn should not have reached lock manager";
  }

  auto txn = txn_holder.transaction();
  auto txn_id = txn->internal().id();
  auto& txn_info = txn_info_[txn_id];
  if (txn->procedure_case() == Transaction::kRemaster) {
    // A remaster txn only has one key K but it acquires locks on
    // (K, RO) and (K, RN) where RO and RN are the old and new regions
    // respectively.
    txn_info.unarrived_lock_requests += 2;
  } else {
    txn_info.unarrived_lock_requests += txn_holder.keys_in_partition().size();
  }
  return txn_info.is_ready();
}

AcquireLocksResult DDRLockManager::AcquireLocks(const TxnHolder& txn_holder) {
  if (txn_holder.keys_in_partition().empty()) {
    LOG(FATAL) << "Empty txn should not have reached lock manager";
  }

  auto txn = txn_holder.transaction();
  auto txn_id = txn->internal().id();
  auto& txn_info = txn_info_[txn_id];

  // Enumerate all locks to be requested
  vector<pair<KeyReplica, LockMode>> locks_to_request;
  if (txn->procedure_case() == Transaction::kRemaster) {
    auto pair = *txn_holder.keys_in_partition().begin();
    auto& key = pair.first;
    auto mode = LockMode::WRITE;
    // Lock on old master if this is the first part of the remaster
    auto master = txn->internal().master_metadata().at(key).master();
    if (txn->remaster().is_new_master_lock_only()) {
      // Lock on new master if this is the second part of the remaster
      master = txn->remaster().new_master();
    }
    auto key_replica = MakeKeyReplica(key, master);
    locks_to_request.emplace_back(move(key_replica), mode);
  } else {
    for (auto& pair : txn_holder.keys_in_partition()) {
      auto& key = pair.first;
      auto mode = pair.second;
      auto master = txn->internal().master_metadata().at(key).master();
      auto key_replica = MakeKeyReplica(key, master);
      locks_to_request.emplace_back(move(key_replica), mode);
    }
  }

  txn_info.unarrived_lock_requests -= locks_to_request.size();

  vector<TxnId> blocking_txns;
  for (auto& pair : locks_to_request) {
    auto& key_replica = pair.first;
    auto mode = pair.second;
    auto& lock_queue_tail = lock_table_[key_replica];

    switch (mode) {
      case LockMode::READ: {
        auto b_txn = lock_queue_tail.AcquireReadLock(txn_id);
        if (b_txn.has_value()) {
          blocking_txns.push_back(b_txn.value());
        }
        break;
      }
      case LockMode::WRITE: {
        auto b_txns = lock_queue_tail.AcquireWriteLock(txn_id);
        blocking_txns.insert(blocking_txns.begin(), b_txns.begin(), b_txns.end());
        break;
      }
      default:
        LOG(FATAL) << "Invalid lock mode";
    }
  }

  // Deduplicate the blocking txns list. We throw away this list eventually
  // so there is no need to erase the extra values at the tail
  std::sort(blocking_txns.begin(), blocking_txns.end());
  auto last = std::unique(blocking_txns.begin(), blocking_txns.end());

  // Add current txn to the waited_by list of each blocking txn
  for (auto b_txn = blocking_txns.begin(); b_txn != last; b_txn++) {
    if (*b_txn == txn_id) {
      VLOG(1) << "Txn " << txn_id << " is trying to acquire the same lock twice";
      continue;
    }
    // The txns returned from the lock table might already leave
    // the lock manager so we need to check for their existence here
    auto b_txn_info = txn_info_.find(*b_txn);
    if (b_txn_info == txn_info_.end()) {
      continue;
    }
    // Let A be a blocking txn of a multi-home txn B. It is possible that
    // two lock-only txns of B both sees A and A is double counted here.
    // However, B is also added twice in the waited_by list of A. Therefore,
    // on releasing A, waiting_for_cnt of B is correctly subtracted.
    txn_info.waiting_for_cnt++;
    b_txn_info->second.waited_by.push_back(txn_id);
  }

  if (txn_info.is_ready()) {
    return AcquireLocksResult::ACQUIRED;
  }
  return AcquireLocksResult::WAITING;
}

AcquireLocksResult DDRLockManager::AcceptTxnAndAcquireLocks(const TxnHolder& txn_holder) {
  AcceptTransaction(txn_holder);
  return AcquireLocks(txn_holder);
}

vector<TxnId> DDRLockManager::ReleaseLocks(const TxnHolder& txn_holder) {
  vector<TxnId> result;
  auto txn = txn_holder.transaction();
  auto txn_id = txn->internal().id();
  auto txn_info_it = txn_info_.find(txn_id);
  if (txn_info_it == txn_info_.end()) {
    return result;
  }
  auto& txn_info = txn_info_it->second;
  if (!txn_info.is_ready()) {
    LOG(FATAL) << "Releasing unready txn is forbidden";
  }
  for (auto blocked_txn_id : txn_info.waited_by) {
    auto it = txn_info_.find(blocked_txn_id);
    if (it == txn_info_.end()) {
      LOG(ERROR) << "Blocked txn " << blocked_txn_id << " does not exist";
      continue;
    }
    auto& blocked_txn = it->second;
    blocked_txn.waiting_for_cnt--;
    if (blocked_txn.is_ready()) {
      // While the waited_by list might contain duplicates, the blocked
      // txn only becomes ready when its last entry in the waited_by list
      // is accounted for.
      result.push_back(blocked_txn_id);
    }
  }
  txn_info_.erase(txn_id);
  return result;
}

void DDRLockManager::GetStats(rapidjson::Document& stats, uint32_t level) const {
  using rapidjson::StringRef;

  auto& alloc = stats.GetAllocator();
  stats.AddMember(StringRef(NUM_TXNS_WAITING_FOR_LOCK), txn_info_.size(), alloc);

  if (level >= 1) {
    // Collect number of locks waited per txn
    // TODO: Give this another name. For this lock manager, this is
    // number of txn waited, not the number of locks.
    stats.AddMember(StringRef(NUM_LOCKS_WAITED_PER_TXN),
                    ToJsonArrayOfKeyValue(
                        txn_info_, [](const auto& info) { return info.waiting_for_cnt; }, alloc),
                    alloc);
  }

  stats.AddMember(StringRef(NUM_LOCKED_KEYS), 0, alloc);
  if (level >= 2) {
    // Collect data from lock tables
    rapidjson::Value lock_table(rapidjson::kArrayType);
    for (const auto& pair : lock_table_) {
      auto& key = pair.first;
      auto& lock_state = pair.second;
      rapidjson::Value entry(rapidjson::kArrayType);
      rapidjson::Value key_json(key.c_str(), alloc);
      entry.PushBack(key_json, alloc)
          .PushBack(lock_state.write_lock_requester().value_or(0), alloc)
          .PushBack(ToJsonArray(lock_state.read_lock_requesters(), alloc), alloc);
      lock_table.PushBack(move(entry), alloc);
    }
    stats.AddMember(StringRef(LOCK_TABLE), move(lock_table), alloc);
  }
}

}  // namespace slog