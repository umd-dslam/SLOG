#pragma once

#include <unordered_map>

#include "module/scheduler_components/remaster_manager.h"

#include "storage/storage.h"

using std::shared_ptr;
using std::unordered_map;


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
    shared_ptr<Storage<Key, Record>> storage);

  virtual VerifyMasterResult VerifyMaster(const TransactionHolder* txn_holder);
  virtual RemasterOccurredResult RemasterOccured(const Key key, const uint32_t remaster_counter);

private:
  /**
   * Compare transaction metadata to stored metadata
   */
  VerifyMasterResult CheckCounters(const TransactionHolder* txn_holder);

  /**
   * Test if the head of this queue can be unblocked
   */
  void TryToUnblock(const uint32_t local_log_machine_id, RemasterOccurredResult& result);

  // Needs access to storage to check counters
  shared_ptr<Storage<Key, Record>> storage_;

  // One queue is kept per local log
  unordered_map<uint32_t, list<const TransactionHolder*>> blocked_queue_;
};

} // namespace slog
