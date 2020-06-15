#pragma once

// Prevent mixing with deprecated version
#ifdef DETERMINISTIC_LOCK_MANAGER
  #error "Only one lock manager can be included"
#endif
#define DETERMINISTIC_LOCK_MANAGER

#include <list>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/json_utils.h"
#include "common/transaction_holder.h"
#include "common/types.h"

using std::list;
using std::shared_ptr;
using std::pair;
using std::unordered_map;
using std::unordered_set;
using std::vector;

namespace slog {

using KeyReplica = string;

enum class AcquireLocksResult {ACQUIRED, WAITING, ABORT};

/**
 * An object of this class represents the locking state of a key.
 * It contains the IDs of transactions that are holding and waiting
 * the lock and the mode of the lock.
 */
class LockState {

public:
  bool AcquireReadLock(TxnId txn_id);
  bool AcquireWriteLock(TxnId txn_id);
  unordered_set<TxnId> Release(TxnId txn_id);
  bool Contains(TxnId txn_id);

  LockMode mode = LockMode::UNLOCKED;

  /* For debugging */
  const unordered_set<TxnId>& GetHolders() const {
    return holders_;
  }

  /* For debugging */
  const list<pair<TxnId, LockMode>>& GetWaiters() const {
    return waiter_queue_;
  }

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
 * Remastering:
 * Locks are taken on the tuple <key, replica>, using the transaction's
 * master metadata. The masters are checked in the worker, so if two
 * transactions hold separate locks for the same key, then one has an
 * incorrect master and will be aborted. Remaster transactions request the
 * locks for both <key, old replica> and <key, new replica>.
 * 
 * TODO: aborts can be detected here, before transactions are dispatched
 */
class DeterministicLockManager {
public:
  /**
   * Counts the number of locks a txn needs.
   * 
   * For MULTI_HOME txns, the number of needed locks before
   * calling this method can be negative due to its LockOnly
   * txn. Calling this function would bring the number of waited
   * locks back to 0, meaning all locks are granted.
   * 
   * @param txn_holder Holder of the transaction to be registered.
   * @return    true if all locks are acquired, false if not and
   *            the transaction is queued up.
   */
  bool AcceptTransaction(const TransactionHolder& txn_holder);

  /**
   * Tries to acquire all locks for a given transaction. If not
   * all locks are acquired, the transaction is queued up to wait
   * for the current holders to release.
   * 
   * @param txn_holder Holder of the transaction whose locks are acquired.
   * @return    true if all locks are acquired, false if not and
   *            the transaction is queued up.
   */
  AcquireLocksResult AcquireLocks(const TransactionHolder& txn_holder);

  /**
   * Convenient method to perform txn registration and 
   * lock acquisition at the same time.
   */
  AcquireLocksResult AcceptTransactionAndAcquireLocks(const TransactionHolder& txn_holder);

  /**
   * Releases all locks that a transaction is holding or waiting for.
   * 
   * @param txn_holder Holder of the transaction whose locks are released.
   *            LockOnly txn is not accepted.
   * @return    A set of IDs of transactions that are able to obtain
   *            all of their locks thanks to this release.
   */
  unordered_set<TxnId> ReleaseLocks(const TransactionHolder& txn_holder);

  /**
   * Gets current statistics of the lock manager
   * 
   * @param stats A JSON object where the statistics are stored into
   */
  void GetStats(rapidjson::Document& stats, uint32_t level) const;

private:
  bool TransactionRequestsNewLock(const TransactionHolder& txn_holder);

  unordered_map<KeyReplica, LockState> lock_table_;
  unordered_map<TxnId, int32_t> num_locks_waited_;
  uint32_t num_locked_keys_ = 0;
};

inline KeyReplica MakeKeyReplica(Key key, uint32_t master) {
  // Note: this is unique, since keys cannot contain spaces
  return key + " " + std::to_string(master);
}

} // namespace slog