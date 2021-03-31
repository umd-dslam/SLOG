#pragma once

// Prevent mixing with deprecated version
#ifdef LOCK_MANAGER
#error "Only one lock manager can be included"
#endif
#define LOCK_MANAGER

#include <list>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/json_utils.h"
#include "common/txn_holder.h"
#include "common/types.h"

using std::list;
using std::pair;
using std::shared_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;

namespace slog {

/**
 * An object of this class represents the locking state of a key.
 * It contains the IDs of transactions that are holding and waiting
 * the lock and the mode of the lock.
 */
class LockState {
 public:
  bool AcquireReadLock(TxnId txn_id);
  bool AcquireWriteLock(TxnId txn_id);
  vector<TxnId> Release(TxnId txn_id);
  bool Contains(TxnId txn_id);

  LockMode mode = LockMode::UNLOCKED;

  /* For debugging */
  const vector<TxnId>& GetHolders() const { return holders_; }

  /* For debugging */
  const list<pair<TxnId, LockMode>>& GetWaiters() const { return waiter_queue_; }

 private:
  vector<TxnId> holders_;
  list<pair<TxnId, LockMode>> waiter_queue_;
};

/**
 * This is a deterministic lock manager which grants locks for transactions
 * in the order that they request. If transaction X, appears before
 * transaction Y in the log, X always gets all locks before Y.
 *
 * RMA stands for Remaster Aware. With this lock manager, we don't need
 * a separate remaster manager.
 *
 * Remastering:
 * Locks are taken on the tuple <key, replica>, using the transaction's
 * master metadata. The masters are checked in the worker, so if two
 * transactions hold separate locks for the same key, then one has an
 * incorrect master and will be aborted. Remaster transactions request the
 * locks for both <key, old replica> and <key, new replica>.
 */
class RMALockManager {
 public:
  /**
   * Tries to acquire all locks for a given transaction. If not
   * all locks are acquired, the transaction is queued up to wait
   * for the current lock holders to release.
   *
   * @param txn The transaction whose locks are acquired.
   * @return    true if all locks are acquired, false if not and
   *            the transaction is queued up.
   */
  AcquireLocksResult AcquireLocks(const Transaction& txn);

  /**
   * Releases all locks that a transaction is holding or waiting for.
   *
   * @param txn_id Id of transaction whose locks are released.
   * @return       A set of IDs of transactions that are able to obtain
   *               all of their locks thanks to this release.
   */
  vector<TxnId> ReleaseLocks(TxnId txn_id);

  /**
   * Gets current statistics of the lock manager
   *
   * @param stats A JSON object where the statistics are stored into
   */
  void GetStats(rapidjson::Document& stats, uint32_t level) const;

 private:
  struct TxnInfo {
    TxnInfo(int num_keys) : num_waiting_for(num_keys) { keys.reserve(num_keys); }

    bool is_ready() const { return num_waiting_for == 0; }

    int num_waiting_for;
    std::vector<Key> keys;
  };
  unordered_map<TxnId, TxnInfo> txn_info_;
  unordered_map<KeyReplica, LockState> lock_table_;
  uint32_t num_locked_keys_ = 0;
};

}  // namespace slog