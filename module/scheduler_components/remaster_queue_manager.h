#pragma once

#include <list>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/types.h"

#include "storage/storage.h"

using std::list;
using std::shared_ptr;
using std::pair;
using std::unordered_map;
using std::unordered_set;
using std::vector;

namespace slog {

enum class VerifyMasterResult {VALID, WAITING, ABORT};


/**
 * A deterministic lock manager grants locks for transactions in
 * the order that they request. If transaction X, appears before
 * transaction Y in the log, thus requesting lock before transaction Y,
 * then X always gets all locks before Y.
 * 
 * The lock manager also conducts the final check of master metadata.
 * If a remaster has occured since the transaction was forwarded, it may
 * need to be restarted.
 */
class RemasterQueueManager {
public:
  RemasterQueueManager(
    ConfigurationPtr config,
    shared_ptr<Storage<Key, Record>> storage);

  /**
   * Checks the counters of the transaction's master metadata.
   * 
   * @param txn Transaction to be checked
   * @return The result of the check.
   * - If Valid, the transaction can be sent for locks.
   * - If Waiting, the counters were ahead (meaning that a remaster
   * has occured at another region before the local). The
   * transaction will be put in a queue to wait for the remaster
   * to be executed locally.
   * - If Aborted, the counters were behind and the transaction
   * needs to be aborted.
   */
  VerifyMasterResult VerifyMaster(const Transaction& txn);

  /**
   * Updates the queue of transactions waiting for remasters,
   * and returns any newly unblocked transactions.
   * 
   * @param key The key that has been remastered
   * @return A queue of transactions that are now unblocked, in the
   * order they were submitted
   */
  list<TxnId> RemasterOccured(Key key);

private:
  ConfigurationPtr config_;
  shared_ptr<Storage<Key, Record>> storage_;
  
  // Queues for the transactions waiting for each key
  unordered_map<Key, list<TxnId>> keys_waiting_remaster_;
  // Status of each transaction (how many remasters of each key it needs)
  unordered_map<TxnId, unordered_map<Key, int32_t>> num_remasters_waited_;
};

} // namespace slog