#include "module/scheduler_components/remaster_queue_manager.h"

#include <glog/logging.h>
#include "common/transaction_utils.h"

using std::make_pair;

namespace slog {


RemasterQueueManager::RemasterQueueManager(ConfigurationPtr config, shared_ptr<Storage<Key, Record>> storage)
  : config_(config), storage_(storage) {}

VerifyMasterResult
RemasterQueueManager::VerifyMaster(const Transaction& txn) {
  auto keys = ExtractKeysInPartition(config_, txn);
  if (keys.empty()) {
    // None of the keys in this txn is in this partition
    return VerifyMasterResult::VALID;
  }

  auto txn_master_metadata = txn.internal().master_metadata();
  if (txn_master_metadata.empty()) { // allow this to not be set for testing
    LOG(WARNING) << "Master metadata empty: txn id " << txn.internal().id();
    return VerifyMasterResult::VALID;
  }

  // keeps track of counters that will need to be waited on
  std::unordered_map<Key, int32_t> ahead_counters;
  for (auto pair : keys) {
    auto key = pair.first;

    Record record;
    bool found = storage_->Read(key, record);
    if (found) {
      CHECK(txn_master_metadata.contains(key))
              << "Master metadata for key \"" << key << "\" is missing";

      auto txn_metadata = txn_master_metadata.at(key);
      auto stored_metadata = record.metadata;

      if (txn_metadata.counter() > stored_metadata.counter) {
        ahead_counters[key] += txn_metadata.counter() - stored_metadata.counter;
      } else if (txn_metadata.counter() < stored_metadata.counter) {
        return VerifyMasterResult::ABORT;
      } else {
        CHECK(txn_metadata.master() == stored_metadata.master)
                << "Metadata is invalid, different masters with the same counter";
      }
    } else { // key not in storage
      // TODO: verify if counter is 0?
    }
  }
  if (!ahead_counters.empty()){
    auto txn_id = txn.internal().id();
    num_remasters_waited_[txn_id] = ahead_counters;

    // add the txn to the overall queue for each key
    for (auto key : ahead_counters) {
      keys_waiting_remaster_[key.first].push_back(txn_id);
    }
    return VerifyMasterResult::WAITING;
  } else {
    return VerifyMasterResult::VALID;
  }
}

list<TxnId> RemasterQueueManager::RemasterOccured(Key key) {
  list<TxnId> unblocked;

  if (keys_waiting_remaster_.count(key) == 0) {
    return unblocked;
  }

  for (auto txn_id : keys_waiting_remaster_[key]) {
    // the number of remasters this txn needs for each key
    auto& waiting_map = num_remasters_waited_[txn_id];

    waiting_map[key] -= 1;
    if (waiting_map[key] == 0) { // no longer waiting on this key
      waiting_map.erase(key);
      if (waiting_map.empty()) { // no longer waiting on any key
        unblocked.push_back(txn_id);
        num_remasters_waited_.erase(txn_id);
      }
    }
  }

  // remove the unblocked txns from the queue
  for (auto txn_id : unblocked) {
    keys_waiting_remaster_[key].remove(txn_id);
    if (keys_waiting_remaster_[key].empty()) {
      keys_waiting_remaster_.erase(key);
    }
  }
  
  return unblocked;
}

} // namespace slog