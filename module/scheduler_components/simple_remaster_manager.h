#pragma once

#include <unordered_map>

#include "module/scheduler_components/remaster_manager.h"

using std::unordered_map;
using std::unordered_set;


namespace slog {

/**
 * Basic, inefficient implimentation of remastering. Transactions are kept in the exact
 * order as their local logs: if a transaction from region 1 is blocked in the queue, all
 * following transactions from region 1 will be blocked behind it
 */
class SimpleRemasterManager :
    public RemasterManager {
public:
  SimpleRemasterManager(
    const shared_ptr<Storage<Key, Record>> storage);

  virtual VerifyMasterResult VerifyMaster(TransactionHolder* txn_holder);
  virtual RemasterOccurredResult RemasterOccured(Key key, uint32_t remaster_counter);
  virtual RemasterOccurredResult ReleaseTransaction(TxnId txn_id);
  /**
   * Release a txn in the queues for specified replicas
   */
  virtual RemasterOccurredResult ReleaseTransaction(TxnId txn_id, const unordered_set<uint32_t>& replicas);

private:
  /**
   * Test if the head of this queue can be unblocked
   */
  void TryToUnblock(uint32_t local_log_machine_id, RemasterOccurredResult& result);

  // Needs access to storage to check counters
  shared_ptr<Storage<Key, Record>> storage_;

  // One queue is kept per local log
  unordered_map<uint32_t, list<TransactionHolder*>> blocked_queue_;
};

} // namespace slog
