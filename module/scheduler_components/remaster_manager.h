#pragma once

#include <list>

#include "common/types.h"
#include "common/transaction_holder.h"

using std::list;

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
};

} // namespace slog
