#pragma once

#include <glog/logging.h>

#include <list>

#include "common/types.h"
#include "module/scheduler_components/txn_holder.h"
#include "storage/storage.h"

using std::list;
using std::shared_ptr;

namespace slog {

enum class VerifyMasterResult { VALID, WAITING, ABORT };
struct RemasterOccurredResult {
  list<Transaction*> unblocked;
  list<Transaction*> should_abort;
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
   * @param txn Transaction to be checked
   * @return The result of the check.
   * - If Valid, the transaction can be sent for locks.
   * - If Waiting, the transaction will be queued until a remaster
   * txn unblocks it
   * - If Aborted, the counters were behind and the transaction
   * needs to be aborted.
   */
  virtual VerifyMasterResult VerifyMaster(Transaction& txn) = 0;

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
   * @return Transactions that are now unblocked
   */
  virtual RemasterOccurredResult ReleaseTransaction(const Transaction& txn) = 0;

  /**
   * Compare transaction metadata to stored metadata, without adding the
   * transaction to any queues
   */
  static VerifyMasterResult CheckCounters(const Transaction& txn, bool filter_by_home,
                                          const shared_ptr<const Storage>& storage) {
    auto waiting = false;
    for (const auto& kv : txn.keys()) {
      const auto& key = kv.key();
      const auto& value = kv.value_entry();
      if (filter_by_home && static_cast<int>(value.metadata().master()) != txn.internal().home()) {
        continue;
      }
      // Get current counter from storage
      uint32_t storage_counter = 0;  // default to 0 for a new key
      Record record;
      bool found = storage->Read(key, record);
      if (found) {
        storage_counter = record.metadata().counter;
      }

      if (value.metadata().counter() < storage_counter) {
        return VerifyMasterResult::ABORT;
      } else if (value.metadata().counter() > storage_counter) {
        waiting = true;
      } else {
        CHECK(value.metadata().master() == record.metadata().master)
            << "Masters don't match for same key \"" << key << "\". In txn: " << value.metadata().master()
            << ". In storage: " << record.metadata().master;
      }
    }

    return waiting ? VerifyMasterResult::WAITING : VerifyMasterResult::VALID;
  }
};

}  // namespace slog
