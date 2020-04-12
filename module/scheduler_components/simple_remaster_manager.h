#pragma once

#include <unordered_map>

#include "module/scheduler_components/remaster_manager.h"

#include "common/transaction_utils.h"

#include "storage/storage.h"

using std::shared_ptr;
using std::unordered_map;


namespace slog {

using TransactionMap = unordered_map<TxnReplicaId, TransactionHolder>;

class SimpleRemasterManager :
    public RemasterManager {
public:
  SimpleRemasterManager(
    shared_ptr<Storage<Key, Record>> storage,
    shared_ptr<TransactionMap> all_txns);

  virtual VerifyMasterResult VerifyMaster(const TxnReplicaId txn_replica_id);
  virtual RemasterOccurredResult RemasterOccured(const Key key, const uint32_t remaster_counter);

private:
  VerifyMasterResult CheckCounters(TransactionHolder& txn_holder);
  void TryToUnblock(const uint32_t local_log_machine_id, RemasterOccurredResult& result);

  shared_ptr<Storage<Key, Record>> storage_;
  unordered_map<uint32_t, list<TxnReplicaId>> blocked_queue_;
  shared_ptr<TransactionMap> all_txns_;

};

} // namespace slog
