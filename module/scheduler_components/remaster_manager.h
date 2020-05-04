#pragma once

#include <list>

#include "common/types.h"
#include "common/transaction_holder.h"

#include "storage/storage.h"

#include <glog/logging.h>

using std::list;
using std::shared_ptr;

namespace slog {

enum class VerifyMasterResult {VALID, WAITING, ABORT};
struct RemasterOccurredResult {
  list<const TransactionHolder*> unblocked;
  list<const TransactionHolder*> should_abort;
};

/**
 * The remaster queue manager conducts the check of master metadata.
 * If a remaster has occured since the transaction was forwarded, it may
 * need to be restarted. If the transaction arrived before a remaster that
 * the forwarder included in the metadata, then it will need to wait.
 */
class RemasterManager {
public:

  /**
   * Checks the counters of the transaction's master metadata.
   * 
   * @param txn_holder Transaction to be checked
   * @return The result of the check.
   * - If Valid, the transaction can be sent for locks.
   * - If Waiting, the transaction will be queued until a remaster
   * txn unblocks it
   * - If Aborted, the counters were behind and the transaction
   * needs to be aborted.
   */
  virtual VerifyMasterResult VerifyMaster(const TransactionHolder* txn_holder) = 0;

  /**
   * Updates the queue of transactions waiting for remasters,
   * and returns any newly unblocked transactions.
   * 
   * @param key The key that has been remastered
   * @param remaster_counter The key's new counter
   * @return A queue of transactions that are now unblocked, in the
   * order they were submitted
   */
  virtual RemasterOccurredResult RemasterOccured(const Key key, const uint32_t remaster_counter) = 0;

  /**
   * Compare transaction metadata to stored metadata, without adding the
   * transaciton to any queues
   */
  static VerifyMasterResult CheckCounters(
      const TransactionHolder* txn_holder,
      shared_ptr<Storage<Key, Record>> storage) {
    auto& keys = txn_holder->KeysInPartition();
    auto& txn_master_metadata = txn_holder->GetTransaction()->internal().master_metadata();
    
    if (txn_master_metadata.empty()) { // This should only be the case for testing
      LOG(WARNING) << "Master metadata empty: txn id " << txn_holder->GetTransaction()->internal().id();
      return VerifyMasterResult::VALID;
    }
    for (auto& key_pair : keys) {
      auto& key = key_pair.first;

      auto txn_counter = txn_master_metadata.at(key).counter();

      // Get current counter from storage
      uint32_t storage_counter = 0; // default to 0 for a new key
      Record record;
      bool found = storage->Read(key, record);
      if (found) {        
        storage_counter = record.metadata.counter;
      }

      if (txn_counter < storage_counter) {
        return VerifyMasterResult::ABORT;
      } else if (txn_counter > storage_counter) {
        return VerifyMasterResult::WAITING;
      } else {
        CHECK(txn_master_metadata.at(key).master() == record.metadata.master)
          << "Masters don't match for same key \"" << key << "\"";
      }
    }
    return VerifyMasterResult::VALID;
  }
};

} // namespace slog
