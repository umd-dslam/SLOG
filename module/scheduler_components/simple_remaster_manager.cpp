#include "module/scheduler_components/simple_remaster_manager.h"

#include <glog/logging.h>

namespace slog {

SimpleRemasterManager::SimpleRemasterManager(
    shared_ptr<const Storage<Key, Record>> storage)
  : storage_(storage) {}

VerifyMasterResult
SimpleRemasterManager::VerifyMaster(const TransactionHolder* txn_holder) {
  auto& keys = txn_holder->KeysInPartition();
  if (keys.empty()) {
    return VerifyMasterResult::VALID;
  }

  auto txn = txn_holder->GetTransaction();
  auto& txn_master_metadata = txn->internal().master_metadata();
  if (txn_master_metadata.empty()) { // This should only be the case for testing
    LOG(WARNING) << "Master metadata empty: txn id " << txn->internal().id();
    return VerifyMasterResult::VALID;
  }

  // Determine which local log this txn is from. Since only single home or
  // lock only txns, all keys will have same master
  auto local_log_machine_id = txn_holder->GetReplicaId();

  // Block this txn behind other txns from same local log
  // TODO: check the counters now? would abort earlier
  if (blocked_queue_.count(local_log_machine_id) && !blocked_queue_[local_log_machine_id].empty()) {
    blocked_queue_[local_log_machine_id].push_back(txn_holder);
    return VerifyMasterResult::WAITING;
  }

  // Test counters
  auto result = CheckCounters(txn_holder, storage_);
  if (result == VerifyMasterResult::WAITING) {
    blocked_queue_[local_log_machine_id].push_back(txn_holder);
  }
  return result;
}

RemasterOccurredResult
SimpleRemasterManager::RemasterOccured(Key remaster_key, uint32_t /* remaster_counter */) {
  RemasterOccurredResult result;
  // Try to unblock each txn at the head of a queue, if it contains the remastered key.
  // Note that multiple queues could contain the same key with different counters
  for (auto& queue_pair : blocked_queue_) {
    if (!queue_pair.second.empty()) {
      auto txn_holder = queue_pair.second.front();
      auto& txn_keys = txn_holder->KeysInPartition();
      for (auto& key_pair : txn_keys) {
        auto& txn_key = key_pair.first;
        if (txn_key == remaster_key) {
          // TODO: check here if counters match, saves an iteration through all keys
          TryToUnblock(queue_pair.first, result);
          break;
        }
      }
    }
  }
  return result;
}

RemasterOccurredResult
SimpleRemasterManager::ReleaseTransaction(const TransactionHolder* txn_holder) {
  auto txn_id = txn_holder->GetTransaction()->internal().id();
  for (auto& p : txn_holder->InvolvedReplicas()) {
    if (blocked_queue_.count(p) == 0 || blocked_queue_[p].empty()) {
      continue;
    }
    auto& queue = blocked_queue_[p];
    for (auto itr = queue.begin(); itr != queue.end(); itr++) {
      if ((*itr)->GetTransaction()->internal().id() == txn_id) {
        queue.erase(itr);
        break; // Any transaction should only occur once per queue
      }
    }
  }
  
  // Note: this must happen after the transaction is released, otherwise it could be returned in
  // the unblocked list
  RemasterOccurredResult result;
  for (auto& p : txn_holder->InvolvedReplicas()) {
    // TODO: only necessary if a removed txn was front of the queue
    TryToUnblock(p, result);
  }
  return result;
}

void SimpleRemasterManager::TryToUnblock(uint32_t local_log_machine_id, RemasterOccurredResult& result) {
  if (blocked_queue_[local_log_machine_id].empty()) {
    return;
  }

  auto& txn_holder = blocked_queue_[local_log_machine_id].front();

  auto counter_result = CheckCounters(txn_holder, storage_);
  if (counter_result == VerifyMasterResult::WAITING) {
    return;
  } else if (counter_result == VerifyMasterResult::VALID) {
    result.unblocked.push_back(txn_holder);
  } else if (counter_result == VerifyMasterResult::ABORT) {
    result.should_abort.push_back(txn_holder);
  }

  // Head of queue has changed
  blocked_queue_[local_log_machine_id].pop_front();

  // Note: queue may be left empty, since there are not many replicas
  TryToUnblock(local_log_machine_id, result);
}

} // namespace slog
