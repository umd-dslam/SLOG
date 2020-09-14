#pragma once

#include <queue>
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
#include "module/scheduler_components/deterministic_lock_manager_deprecated.h"
#include "module/scheduler_components/simple_remaster_manager.h"
#elif defined(REMASTER_PROTOCOL_PER_KEY)
#include "module/scheduler_components/deterministic_lock_manager_deprecated.h"
#include "module/scheduler_components/per_key_remaster_manager.h"
#else
#include "module/scheduler_components/deterministic_lock_manager.h"
#endif

namespace slog {

// TODO: extend Scheduler from NetworkedModule to simply the code
class Scheduler : public Module {
public:
  static const string WORKERS_ENDPOINT;
  static const uint32_t WORKER_LOAD_THRESHOLD;

  Scheduler(
      const ConfigurationPtr& config,
      const std::shared_ptr<Broker>& broker,
      const std::shared_ptr<Storage<Key, Record>>& storage);

  void SetUp() final;

  void Loop() final;

private:
  friend class Worker;

  // A marker for the special log containing only multi-home txn
  const uint32_t kMultiHomeTxnLogMarker;

  void HandleInternalRequest(internal::Request&& req);
  void HandleResponseFromWorker(const internal::WorkerResponse& response);
  void SendToCoordinatingServer(TxnId txn_id);

  bool HasMessageFromChannel() const;
  bool HasMessageFromWorker() const;

  void ProcessForwardBatch(internal::ForwardBatch* forward_batch);
  void ProcessRemoteReadResult(internal::Request&& request);
  void ProcessStatsRequest(const internal::StatsRequest& stats_request);

  void ProcessNextBatch(BatchPtr&& batch);

  // Check that remaster txn doesn't keep key at same master
  bool MaybeAbortRemasterTransaction(Transaction* txn);

  // Place a transaction in a holder if it has keys in this partition
  bool AcceptTransaction(Transaction* txn);

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
  // Send single-home and lock-only transactions for counter checking
  void SendToRemasterManager(TransactionHolder* txn_holder);
  // Send transactions to lock manager or abort them
  void ProcessRemasterResult(RemasterOccurredResult result);
#endif /* defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY) */

  // Send all transactions for locks, multi-home transactions are only registered
  void SendToLockManager(const TransactionHolder* txn_holder);
  void AcquireLocksAndProcessResult(const TransactionHolder* txn_holder);

  // Prepare transaction to be sent to worker
  void DispatchTransaction(TxnId txn_id);
  void SendToWorker(internal::Request&& req, const string& worker);

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
  // Multicast the remote read abort to active partitions
  void SendAbortToPartitions(TxnId txn_id);
  // Return the transaction to the server if lock-only transactions and 
  // remote reads are received
  void MaybeFinishAbort(TxnId txn_id);

  ConfigurationPtr config_;
  zmq::socket_t pull_socket_;
  zmq::socket_t worker_socket_;
  std::vector<zmq::pollitem_t> poll_items_;
  std::vector<string> worker_identities_;
  std::vector<unique_ptr<ModuleRunner>> workers_;
  size_t next_worker_;

  Sender sender_;

  BatchLog single_home_log_;
  BatchLog multi_home_log_;
  
#if defined(REMASTER_PROTOCOL_SIMPLE)
  DeterministicLockManagerDeprecated lock_manager_;
  SimpleRemasterManager remaster_manager_;
#elif defined(REMASTER_PROTOCOL_PER_KEY)
  DeterministicLockManagerDeprecated lock_manager_;
  PerKeyRemasterManager remaster_manager_;
#else
  DeterministicLockManager lock_manager_;
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
};

} // namespace slog