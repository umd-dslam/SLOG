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

enum class LockMode { UNLOCKED, READ, WRITE };
// enum LockRequestResult { Success, Fail, RemasterAbort, RemasterWait };
enum VerifyMasterResult {Valid = 1, Waiting = 2, Aborted = 3};


/**
 * The object of this class represent the locking state of a key.
 * It contains the IDs of transactions that are holding and waiting
 * the lock and the mode of the lock being held with.
 */
class LockState {
public:
  bool AcquireReadLock(TxnId txn_id);
  bool AcquireWriteLock(TxnId txn_id);
  unordered_set<TxnId> Release(TxnId txn_id);
  bool IsQueued(TxnId txn_id);

  LockMode mode = LockMode::UNLOCKED;

private:
  unordered_set<TxnId> holders_;
  unordered_set<TxnId> waiters_;
  list<pair<TxnId, LockMode>> waiter_queue_;

};

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
class DeterministicLockManager {
public:
  DeterministicLockManager(
    ConfigurationPtr config,
    shared_ptr<Storage<Key, Record>> storage);

  /**
   * Counts the number of locks a txn needs.
   * 
   * For MULTI_HOME txns, the number of needed locks before
   * calling this method can be negative due to its LockOnly
   * txn. Calling this function would bring the number of waited
   * locks back to 0, meaning all locks are granted.
   * 
   * @param txn The transaction to be registered.
   * @return    true if all locks are acquired, false if not and
   *            the transaction is queued up.
   */
  bool RegisterTxn(const Transaction& txn);

  /**
   * Tries to acquire all locks for a given transaction. If not
   * all locks are acquired, the transaction is queued up to wait
   * for the current holders to release.
   * 
   * @param txn The transaction whose locks are acquired.
   * @return    true if all locks are acquired, false if not and
   *            the transaction is queued up.
   */
  bool AcquireLocks(const Transaction& txn);

  /**
   * Convenient method to perform txn registration and 
   * lock acquisition at the same time.
   */
  bool RegisterTxnAndAcquireLocks(const Transaction& txn);

  /**
   * Releases all locks that a transaction is holding or waiting for.
   * 
   * @param txn The transaction whose locks are released.
   *            LockOnly txn is not accepted.
   * @return    A set of IDs of transactions that are able to obtain
   *            all of their locks thanks to this release.
   */
  unordered_set<TxnId> ReleaseLocks(const Transaction& txn);

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
  vector<pair<Key, LockMode>> ExtractKeys(const Transaction& txn);

  ConfigurationPtr config_;
  shared_ptr<Storage<Key, Record>> storage_;
  unordered_map<Key, LockState> lock_table_;
  unordered_map<TxnId, int32_t> num_locks_waited_;
  
  // Queues for the transactions waiting for each key
  unordered_map<Key, list<TxnId>> keys_waiting_remaster_;
  // Status of each transaction (how many remasters of each key it needs)
  unordered_map<TxnId, unordered_map<Key, int32_t>> num_remasters_waited_;
};

} // namespace slog