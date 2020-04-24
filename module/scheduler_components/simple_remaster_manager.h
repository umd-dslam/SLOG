#pragma once

#include <unordered_map>

#include "module/scheduler_components/remaster_manager.h"

#include "common/transaction_holder.h"

#include "storage/storage.h"

using std::shared_ptr;
using std::unordered_map;


namespace slog {

using TransactionMap = unordered_map<TxnId, TransactionHolder>;

/**
 * Basic, inefficient implimentation of remastering. Transactions are kept in the exact
 * order as their local logs: if a transaction from region 1 is blocked in the queue, all
 * following transactions from region 1 will be blocked behind it
 */
class SimpleRemasterManager :
    public RemasterManager {
public:
  SimpleRemasterManager(
    shared_ptr<Storage<Key, Record>> storage,
    shared_ptr<TransactionMap> all_txns);

  virtual VerifyMasterResult VerifyMaster(const TxnReplicaId txn_replica_id);
  virtual RemasterOccurredResult RemasterOccured(const Key key, const uint32_t remaster_counter);

private:
  /**
   * Compare transaction metadata to stored metadata
   */
  VerifyMasterResult CheckCounters(const TxnReplicaId txn_replica_id);

  /**
   * Test if the head of this queue can be unblocked
   */
  void TryToUnblock(const uint32_t local_log_machine_id, RemasterOccurredResult& result);

  const KeyList& GetKeys(const TxnReplicaId txn_replica_id);

  shared_ptr<Storage<Key, Record>> storage_;
  shared_ptr<TransactionMap> all_txns_;

  // One queue is kept per local log
  unordered_map<uint32_t, list<TxnReplicaId>> blocked_queue_;
};

} // namespace slog
