#pragma once

#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/configuration.h"
#include "common/transaction_holder.h"
#include "common/types.h"
#include "connection/broker.h"
#include "data_structure/batch_log.h"
#include "module/base/basic_module.h"
#include "module/scheduler_components/batch_interleaver.h"
#include "module/scheduler_components/deterministic_lock_manager.h"
#include "module/scheduler_components/simple_remaster_manager.h"
#include "module/scheduler_components/worker.h"
#include "storage/storage.h"

using std::shared_ptr;
using std::queue;
using std::unique_ptr;
using std::map;
using std::unordered_map;
using std::unordered_set;
using std::vector;

namespace slog {

class Scheduler : public Module, ChannelHolder {
public:
  static const string WORKERS_ENDPOINT;
  static const uint32_t WORKER_LOAD_THRESHOLD;

  Scheduler(
      ConfigurationPtr config,
      zmq::context_t& context,
      Broker& broker,
      shared_ptr<Storage<Key, Record>> storage);

  void SetUp() final;

  void Loop() final;

private:
  friend class Worker;

  // A marker for the special log containing only multi-home txn
  const uint32_t kMultiHomeTxnLogMarker;

  void HandleInternalRequest(
    internal::Request&& req,
    const string& from_machine_id);
  void HandleResponseFromWorker(const internal::WorkerResponse& response);
  void SendToCoordinatingServer(TxnId txn_id);

  bool HasMessageFromChannel() const;
  bool HasMessageFromWorker() const;

  void ProcessForwardBatch(internal::ForwardBatch* forward_batch, const string& from_machine_id);
  void ProcessLocalQueueOrder(const internal::LocalQueueOrder& order);
  void ProcessRemoteReadResult(internal::Request&& request);
  void ProcessStatsRequest(const internal::StatsRequest& stats_request);

  void MaybeUpdateLocalLog();
  void MaybeProcessNextBatchesFromGlobalLog();

  // Place a transaction in a holder if it has keys in this partition
  bool AcceptTransaction(Transaction* txn);
  // Send single-home and lock-only transactions for counter checking
  void SendToRemasterManager(TransactionHolder* txn_holder);
  // Send transactions to lock manager or abort them
  void ProcessRemasterResult(RemasterOccurredResult result);
  // Send all transactions for locks, multi-home transactions are only registered
  void SendToLockManager(const TransactionHolder* txn_holder);

  // Prepare transaction to be sent to worker
  void DispatchTransaction(TxnId txn_id);
  void SendToWorker(internal::Request&& req, const string& worker);

  /**
   * Once a transaction is sent to a worker, the worker will manage an abort.
   * If a remaster abort occurs at this partition or if a remote read abort
   * is received before the transaction is dispatched, then the abort is handled
   * here.
   * 
   * Aborts are triggered once on every partition. They send remote read aborts
   * to active partitions, then delay until all lock-onlys are collected
   * (if multi-home) and all remote reads are collected (if multi-partition and
   * an active partition)
   */
  
  // Start the abort. Only be called once per transaction
  void TriggerPreDispatchAbort(TxnId txn_id);
  // Add a single or multi-home transaction to an abort that started before it
  // arrived
  void AddTransactionToAbort(TxnId txn_id);
  // Add a lock-only transaction to an abort that started before it arrived
  void AddLockOnlyTransactionToAbort(TxnIdReplicaIdPair txn_replica_id);
  // Abort lock-only transactions from the remaster manager and lock manager
  void CollectLockOnlyTransactionsForAbort(TxnId txn_id);
  // Multicast the remote read abort to active partitions
  void SendAbortToPartitions(TxnId txn_id);
  // Return the transaction to the server if lock-only transactions and 
  // remote reads are received
  void MaybeFinishAbort(TxnId txn_id);

  ConfigurationPtr config_;
  zmq::socket_t worker_socket_;
  vector<zmq::pollitem_t> poll_items_;
  vector<string> worker_identities_;
  vector<unique_ptr<ModuleRunner>> workers_;
  size_t next_worker_;

  unordered_map<uint32_t, BatchLog> all_logs_;
  BatchInterleaver local_interleaver_;
  DeterministicLockManager lock_manager_;
  SimpleRemasterManager remaster_manager_;

  unordered_map<TxnId, TransactionHolder> all_txns_;
  
  /**
   * Lock-only transactions are kept here during remaster checking and locking.
   * This map is also used to track which LOs have arrived during an abort, which means
   * that LOs should not be removed until the txn is dispatched.
   */
  map<TxnIdReplicaIdPair, TransactionHolder> lock_only_txns_;

  unordered_set<TxnId> aborting_txns_;
  /**
   * Stores how many lock-only transactions need aborting during
   * mutli-home abort
   * 
   * Note: can be negative, if lock-onlys abort before the multi-home
   */
  unordered_map<TxnId, int32_t> mh_abort_waiting_on_;
};

} // namespace slog