#include "module/scheduler_components/simple_remaster_manager.h"

#include <glog/logging.h>

namespace slog {

SimpleRemasterManager::SimpleRemasterManager(const shared_ptr<const Storage>& storage) : storage_(storage) {}

VerifyMasterResult SimpleRemasterManager::VerifyMaster(Transaction& txn) {
  if (txn.keys().empty()) {
    return VerifyMasterResult::VALID;
  }

  // Block this txn behind other txns from same local log
  // TODO: check the counters now? would abort earlier
  auto it = blocked_queue_.find(txn.internal().home());
  if (it != blocked_queue_.end() && !it->second.empty()) {
    it->second.push_back(&txn);
    return VerifyMasterResult::WAITING;
  }

  // Test counters
  auto result = CheckCounters(txn, true, storage_);
  if (result == VerifyMasterResult::WAITING) {
    blocked_queue_[txn.internal().home()].push_back(&txn);
  }
  return result;
}

RemasterOccurredResult SimpleRemasterManager::RemasterOccured(const Key& remaster_key,
                                                              uint32_t /* remaster_counter */) {
  RemasterOccurredResult result;
  // Try to unblock each txn at the head of a queue, if it contains the remastered key.
  // Note that multiple queues could contain the same key with different counters
  for (auto& queue_pair : blocked_queue_) {
    if (!queue_pair.second.empty()) {
      auto txn = queue_pair.second.front();
      for (const auto& kv : txn->keys()) {
        if (static_cast<int>(kv.value_entry().metadata().master()) == txn->internal().home() &&
            kv.key() == remaster_key) {
          // TODO: check here if counters match, saves an iteration through all keys
          TryToUnblock(queue_pair.first, result);
          break;
        }
      }
    }
  }
  return result;
}

RemasterOccurredResult SimpleRemasterManager::ReleaseTransaction(const Transaction& txn) {
  auto& txn_internal = txn.internal();
  for (auto replica : txn_internal.involved_replicas()) {
    auto it = blocked_queue_.find(replica);
    if (it == blocked_queue_.end() || it->second.empty()) {
      continue;
    }
    auto& queue = it->second;
    for (auto itr = queue.begin(); itr != queue.end(); itr++) {
      if ((*itr)->internal().id() == txn_internal.id()) {
        queue.erase(itr);
        break;  // Any transaction should only occur once per queue
      }
    }
  }

  // Note: this must happen after the transaction is released, otherwise it could be returned in
  // the unblocked list
  RemasterOccurredResult result;
  for (auto replica : txn_internal.involved_replicas()) {
    // TODO: only necessary if a removed txn was front of the queue
    TryToUnblock(replica, result);
  }
  return result;
}

void SimpleRemasterManager::TryToUnblock(uint32_t local_log_machine_id, RemasterOccurredResult& result) {
  auto it = blocked_queue_.find(local_log_machine_id);
  if (it == blocked_queue_.end() || it->second.empty()) {
    return;
  }

  auto lo_txn = it->second.front();

  auto counter_result = CheckCounters(*lo_txn, true, storage_);
  if (counter_result == VerifyMasterResult::WAITING) {
    return;
  } else if (counter_result == VerifyMasterResult::VALID) {
    result.unblocked.push_back(lo_txn);
  } else if (counter_result == VerifyMasterResult::ABORT) {
    result.should_abort.push_back(lo_txn);
  }

  // Head of queue has changed
  it->second.pop_front();

  // Note: queue may be left empty, since there are not many replicas
  TryToUnblock(local_log_machine_id, result);
}

}  // namespace slog
