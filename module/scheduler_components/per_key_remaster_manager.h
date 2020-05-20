#pragma once

#include <unordered_map>

#include "module/scheduler_components/remaster_manager.h"

using std::unordered_map;
using std::unordered_set;
using std::pair;


namespace slog {

/**
 * Basic, inefficient implimentation of remastering. Transactions are kept in the exact
 * order as their local logs: if a transaction from region 1 is blocked in the queue, all
 * following transactions from region 1 will be blocked behind it
 */
class PerKeyRemasterManager :
    public RemasterManager {
public:
  PerKeyRemasterManager(
    shared_ptr<const Storage<Key, Record>> storage);

  virtual VerifyMasterResult VerifyMaster(const TransactionHolder* txn_holder);
  virtual RemasterOccurredResult RemasterOccured(Key key, uint32_t remaster_counter);
  virtual RemasterOccurredResult ReleaseTransaction(TxnId txn_id);
  /**
   * Release a txn in the queues for specified replicas
   */
  virtual RemasterOccurredResult ReleaseTransaction(const TransactionHolder* txn_holder);

private:
  /**
   * Insert the key into the priority queue sorted by counter. This way remasters can
   * unblock txns starting from the front of the queue, and stop when they reach a
   * larger counter.
   */
  void InsertIntoBlockedQueue(Key key, uint32_t counter, const TransactionHolder* txn_holder);

  /**
   * Test if the head of this queue can be unblocked
   */
  void TryToUnblock(Key unblocked_key, RemasterOccurredResult& result);

  // VerifyMasterResult CheckCounters(const TransactionHolder* txn_holder);

  // Needs access to storage to check counters
  shared_ptr<const Storage<Key, Record>> storage_;

  // Priority queues for the transactions waiting for each key. Lowest counters first, earliest
  // arrivals break tie
  unordered_map<Key, list<pair<const TransactionHolder*, uint32_t>>> blocked_queue_;
};

} // namespace slog
