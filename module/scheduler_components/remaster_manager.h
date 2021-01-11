#pragma once

#include <glog/logging.h>

#include <list>

#include "common/txn_holder.h"
#include "common/types.h"
#include "storage/storage.h"

using std::list;
using std::shared_ptr;

namespace slog {

enum class VerifyMasterResult { VALID, WAITING, ABORT };
struct RemasterOccurredResult {
  list<const TxnHolder*> unblocked;
  list<const TxnHolder*> should_abort;
};

/**
 * The remaster queue manager conducts the check of master metadata.
 * If a remaster has occured since the transaction was forwarded, it may
 * need to be restarted. If the transaction arrived before a remaster that
 * the forwarder included in the metadata, then it will need to wait.
 */
class RemasterManager {
 public:
  virtual ~RemasterManager() = default;

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
  virtual VerifyMasterResult VerifyMaster(const TxnHolder& txn_holder) = 0;

  /**
   * Updates the queue of transactions waiting for remasters,
   * and returns any newly unblocked transactions.
   *
   * @param key The key that has been remastered
   * @param remaster_counter The key's new counter
   * @return A queue of transactions that are now unblocked, in the
   * order they were submitted
   */
  virtual RemasterOccurredResult RemasterOccured(const Key& key, uint32_t remaster_counter) = 0;

  /**
   * Release a transaction from remaster queues. It's guaranteed that the released transaction
   * will not be in the returned result.
   *
   * @param txn_id Transaction to be checked
   * @return Transactions that are now unblocked
   */
  virtual RemasterOccurredResult ReleaseTransaction(const TxnHolder& txn_holder) = 0;

  /**
   * Compare transaction metadata to stored metadata, without adding the
   * transaction to any queues
   */
  static VerifyMasterResult CheckCounters(const TxnHolder& txn_holder,
                                          const shared_ptr<const Storage<Key, Record>>& storage) {
    auto& keys = txn_holder.keys_in_partition();
    auto& txn_master_metadata = txn_holder.transaction()->internal().master_metadata();

    if (txn_master_metadata.empty()) {  // This should only be the case for testing
      LOG(WARNING) << "Master metadata empty: txn id " << txn_holder.transaction()->internal().id();
      return VerifyMasterResult::VALID;
    }

    auto waiting = false;
    for (auto& key_pair : keys) {
      auto& key = key_pair.first;

      auto txn_counter = txn_master_metadata.at(key).counter();

      // Get current counter from storage
      uint32_t storage_counter = 0;  // default to 0 for a new key
      Record record;
      bool found = storage->Read(key, record);
      if (found) {
        storage_counter = record.metadata.counter;
      }

      if (txn_counter < storage_counter) {
        return VerifyMasterResult::ABORT;
      } else if (txn_counter > storage_counter) {
        waiting = true;
      } else {
        CHECK(txn_master_metadata.at(key).master() == record.metadata.master)
            << "Masters don't match for same key \"" << key << "\". In txn: " << txn_master_metadata.at(key).master()
            << ". In storage: " << record.metadata.master;
      }
    }

    if (waiting) {
      return VerifyMasterResult::WAITING;
    } else {
      return VerifyMasterResult::VALID;
    }
  }
};

}  // namespace slog
