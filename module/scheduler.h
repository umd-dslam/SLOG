#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/configuration.h"
#include "common/transaction_holder.h"
#include "common/types.h"
#include "connection/broker.h"
#include "connection/sender.h"
#include "data_structure/batch_log.h"
#include "module/scheduler_components/worker.h"
#include "storage/storage.h"

#if defined(REMASTER_PROTOCOL_SIMPLE)
#include "module/scheduler_components/lock_manager_deprecated.h"
#include "module/scheduler_components/simple_remaster_manager.h"
#elif defined(REMASTER_PROTOCOL_PER_KEY)
#include "module/scheduler_components/lock_manager_deprecated.h"
#include "module/scheduler_components/per_key_remaster_manager.h"
#else
#include "module/scheduler_components/lock_manager.h"
#endif

namespace slog {

class Scheduler : public NetworkedModule {
public:
  Scheduler(
      const ConfigurationPtr& config,
      const std::shared_ptr<Broker>& broker,
      const std::shared_ptr<Storage<Key, Record>>& storage,
      int poll_timeout_ms = kModuleTimeoutMs);

protected:
  void Initialize() final;

  void HandleInternalRequest(ReusableRequest&& req, MachineId) final;
  void HandleInternalResponse(ReusableResponse&& res, MachineId) final;

private:
  void ProcessRemoteReadResult(ReusableRequest&& req);
  void ProcessStatsRequest(const internal::StatsRequest& stats_request);
  void ProcessTransaction(ReusableRequest&& txn);

#ifdef ENABLE_REMASTER
  // Check that remaster txn doesn't keep key at same master
  bool MaybeAbortRemasterTransaction(Transaction* txn);
#endif

  // Place a transaction in a holder if it has keys in this partition
  bool AcceptTransaction(ReusableRequest&& req);

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
  // Send single-home and lock-only transactions for counter checking
  void SendToRemasterManager(TransactionHolder* txn_holder);
  // Send transactions to lock manager or abort them
  void ProcessRemasterResult(RemasterOccurredResult result);
#endif /* defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY) */

  // Send all transactions for locks, multi-home transactions are only registered
  void SendToLockManager(const TransactionHolder* txn_holder);
  void AcquireLocksAndProcessResult(const TransactionHolder* txn_holder);

  // Send txn to worker. If this is a one-way dispatch, a copy of the txn
  // holder will be created and owned by the worker.
  void Dispatch(TxnId txn_id, bool one_way = false);

  /**
   * Aborts
   * 
   * Once a transaction is sent to a worker, the worker will manage an abort.
   * If a remaster abort occurs at this partition or if a remote read abort
   * is received before the transaction is dispatched, then the abort is handled
   * here.
   * 
   * Aborts are triggered once on every partition. Once the main transaction
   * arrives, it's returned to the coordinating server and remote read aborts
   * are sent to every active remote partition.
   * 
   * Before the transaction data is erased, we wait to collect all
   * - lock-onlys (if multi-home)
   * - remote reads (if multi-partition and an active partition)
   */

  // Start the abort. Only be called once per transaction
  void TriggerPreDispatchAbort(TxnId txn_id);
  // Add a single or multi-home transaction to an abort that started before it
  // arrived
  bool MaybeContinuePreDispatchAbort(TxnId txn_id);
  // Add a lock-only transaction to an abort that started before it arrived
  bool MaybeContinuePreDispatchAbortLockOnly(TxnIdReplicaIdPair txn_replica_id);
  // Abort lock-only transactions from the remaster manager and lock manager
  void CollectLockOnlyTransactionsForAbort(TxnId txn_id);
  // Return the transaction to the server if lock-only transactions and 
  // remote reads are received
  void MaybeFinishAbort(TxnId txn_id);

  ConfigurationPtr config_;
  
#if defined(REMASTER_PROTOCOL_SIMPLE)
  LockManagerDeprecated lock_manager_;
  SimpleRemasterManager remaster_manager_;
#elif defined(REMASTER_PROTOCOL_PER_KEY)
  LockManagerDeprecated lock_manager_;
  PerKeyRemasterManager remaster_manager_;
#else
  LockManager lock_manager_;
#endif /* REMASTER_PROTOCOL_COUNTERLESS */

  std::unordered_map<TxnId, TransactionHolder> all_txns_;
  
  /**
   * Lock-only transactions are kept here during remaster checking and locking.
   * This map is also used to track which LOs have arrived during an abort, which means
   * that LOs should not be removed until the txn is dispatched.
   */
  std::map<TxnIdReplicaIdPair, TransactionHolder> lock_only_txns_;

  /**
   * Transactions that are in the process of aborting
   */
  std::unordered_set<TxnId> aborting_txns_;
  /**
   * Stores how many lock-only transactions are yet to arrive during
   * mutli-home abort
   * Note: can be negative, if lock-onlys abort before the multi-home
   */
  std::unordered_map<TxnId, int32_t> mh_abort_waiting_on_;

  std::vector<string> worker_identities_;
  // This must be defined at the end so that the workers exit before any resources
  // in the scheduler is destroyed
  std::vector<unique_ptr<ModuleRunner>> workers_;
};

} // namespace slog