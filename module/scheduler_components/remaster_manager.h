#pragma once

#include <list>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/types.h"
#include "common/transaction_utils.h"

#include "storage/storage.h"

using std::list;
using std::shared_ptr;
using std::pair;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using std::reference_wrapper;

namespace slog {

using TransactionMap = unordered_map<TxnReplicaId, TransactionHolder>;

// using TransactionMap = unordered_map<TxnId, TransactionHolder>;
enum class VerifyMasterResult {VALID, WAITING, ABORT};

/**
 * The remaster queue manager conducts the check of master metadata.
 * If a remaster has occured since the transaction was forwarded, it may
 * need to be restarted. If the transaction arrived before a remaster that
 * the forwarder included in the metadata, then it will need to wait.
 */
class RemasterManager {
public:
  RemasterManager(
    ConfigurationPtr config,
    shared_ptr<Storage<Key, Record>> storage,
    shared_ptr<TransactionMap> all_txns);

  /**
   * Checks the counters of the transaction's master metadata.
   * 
   * @param txn Transaction to be checked
   * @return The result of the check.
   * - If Valid, the transaction can be sent for locks.
   * - If Waiting, the counters were ahead (meaning that a remaster
   * has occured at another region before the local). The
   * transaction will be put in a queue to wait for the remaster
   * to be executed locally. Or, the transaction is blocked behind
   * another transaction that is waiting.
   * - If Aborted, the counters were behind and the transaction
   * needs to be aborted.
   */
  VerifyMasterResult VerifyMaster(const TxnReplicaId txn_replica_id);

  /**
   * Updates the queue of transactions waiting for remasters,
   * and returns any newly unblocked transactions.
   * 
   * @param key The key that has been remastered
   * @return A queue of transactions that are now unblocked, in the
   * order they were submitted
   */
  list<TxnReplicaId> RemasterOccured(const Key key, const uint32_t remaster_counter);

private:
  /**
   * Insert the key into the priority queue sorted by counter. This way remasters can
   * unblock txns starting from the front of the queue, and stop when they reach a
   * larger counter.
   */
  void InsertIntoBlockedQueue(const Key key, const uint32_t counter, const TxnReplicaId txn_replica_id);

  /**
   * A txn can be unblocked if it is at the front of the indirectly_blocked_queue for
   * all of its keys. This function will try to unblock the txn at the key specified,
   * and if successful it will continue recursively on the keys of the transaction that
   * was unblocked.
   */
  void TryToUnblock(const Key unblocked_key, list<TxnReplicaId>& unblocked);

  ConfigurationPtr config_;
  shared_ptr<Storage<Key, Record>> storage_;
  
  // Priority queues for the transactions waiting for each key. Lowest counters first, earliest
  // arrivals break tie
  unordered_map<Key, list<pair<TxnReplicaId, uint32_t>>> blocked_queue_;

  // Cached counter value for each key that's currently in the queue
  unordered_map<Key, uint32_t> counters_;

  shared_ptr<TransactionMap> all_txns_;
  
};

} // namespace slog