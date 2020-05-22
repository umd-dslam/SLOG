#pragma once

#include <unordered_map>

#include "module/scheduler_components/remaster_manager.h"

using std::unordered_map;
using std::unordered_set;
using std::pair;


namespace slog {

/**
 * Alternative implementation of remastering. Txns only block if they have a high counter, or if
 * a transaction with a high counter arrived first with an overlapping read/write set.
 * With small transactions, should acheive lower latency than SimpleRemasterManager. For large
 * transactions, the overhead of creating queues may be too high.
 */
class PerKeyRemasterManager :
    public RemasterManager {
public:
  PerKeyRemasterManager(
    shared_ptr<const Storage<Key, Record>> storage);

  virtual VerifyMasterResult VerifyMaster(const TransactionHolder* txn_holder);
  virtual RemasterOccurredResult RemasterOccured(const Key& key, uint32_t remaster_counter);
  virtual RemasterOccurredResult ReleaseTransaction(const TransactionHolder* txn_holder);

private:
  /**
   * Insert the key into the priority queue sorted by counter. This way remasters can
   * unblock txns starting from the front of the queue, and stop when they reach a
   * larger counter.
   */
  void InsertIntoBlockedQueue(const Key& key, uint32_t counter, const TransactionHolder* txn_holder);

  /**
   * Test if the head of this queue can be unblocked
   */
  void TryToUnblock(const Key& unblocked_key, RemasterOccurredResult& result);

  // Needs access to storage to check counters
  shared_ptr<const Storage<Key, Record>> storage_;

  // Priority queues for the transactions waiting for each key. Lowest counters first, earliest
  // arrivals break tie
  unordered_map<Key, list<pair<const TransactionHolder*, uint32_t>>> blocked_queue_;
};

} // namespace slog