#pragma once

#include <list>
#include <unordered_map>
#include <unordered_set>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/types.h"

using std::list;
using std::shared_ptr;
using std::pair;
using std::unordered_map;
using std::unordered_set;

namespace slog {

enum class LockMode { UNLOCKED, READ, WRITE };

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

  LockMode mode = LockMode::UNLOCKED;

private:
  unordered_set<TxnId> holders_;
  list<pair<TxnId, LockMode>> waiters_;
};

/**
 * A deterministic lock manager grants locks for transactions in
 * the order that they request. If transaction X, appears before
 * transaction Y in the log, thus requesting lock before transaction Y,
 * then X always gets all locks before Y.
 */
class DeterministicLockManager {
public:
  DeterministicLockManager(shared_ptr<Configuration> config);

  /**
   * Tries to acquire all locks for a given transaction. If not
   * all locks are acquired, the transaction is queued up to wait
   * for the current holders to release.
   * @param txn The transaction whose locks are acquired
   * @return    true if all locks are acquired, false if not and
   *            the transaction is queued up
   */
  bool AcquireLocks(const Transaction& txn);

  /**
   * Releases all locks that a transaction is holding or waiting for.
   * @param txn The transaction whose locks are released
   * @return    A set of IDs of transactions that are able to obtain
   *            all of their locks thanks to this release.
   */
  unordered_set<TxnId> ReleaseLocks(const Transaction& txn);

private:
  shared_ptr<Configuration> config_;
  unordered_map<Key, LockState> lock_table_;
  unordered_map<TxnId, uint32_t> num_locks_waited_;
};

} // namespace slog