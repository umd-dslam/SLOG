#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/configuration.h"
#include "common/txn_holder.h"
#include "common/types.h"
#include "connection/broker.h"
#include "connection/sender.h"
#include "data_structure/batch_log.h"
#include "module/scheduler_components/worker.h"
#include "storage/storage.h"

#if defined(REMASTER_PROTOCOL_SIMPLE)
#include "module/scheduler_components/simple_remaster_manager.h"
#if !defined(LOCK_MANAGER_OLD)
#error "SIMPLE remaster protocol is only compatible with OLD lock manager"
#endif
#elif defined(REMASTER_PROTOCOL_PER_KEY)
#include "module/scheduler_components/per_key_remaster_manager.h"
#if !defined(LOCK_MANAGER_OLD)
#error "PER_KEY remaster protocol is only compatible with OLD lock manager"
#endif
#elif defined(REMASTER_PROTOCOL_COUNTERLESS) && defined(LOCK_MANAGER_OLD)
#error "COUNTERLESS remaster protocol is not compatible with OLD lock manager"
#endif

#if defined(LOCK_MANAGER_OLD)
#include "module/scheduler_components/old_lock_manager.h"
#elif defined(LOCK_MANAGER_DDR)
#include "module/scheduler_components/ddr_lock_manager.h"
#else
#include "module/scheduler_components/rma_lock_manager.h"
#endif

namespace slog {

class Scheduler : public NetworkedModule {
 public:
  Scheduler(const ConfigurationPtr& config, const std::shared_ptr<Broker>& broker,
            const std::shared_ptr<Storage<Key, Record>>& storage,
            std::chrono::milliseconds poll_timeout = kModuleTimeout);

 protected:
  void Initialize() final;
  std::vector<zmq::socket_t> InitializeCustomSockets() final;

  void HandleInternalRequest(EnvelopePtr&& env) final;
  void HandleCustomSocket(zmq::socket_t& socket, size_t) final;

 private:
  void ProcessRemoteReadResult(EnvelopePtr&& env);
  void ProcessStatsRequest(const internal::StatsRequest& stats_request);
  void ProcessTransaction(EnvelopePtr&& env);

#ifdef ENABLE_REMASTER
  // Check that remaster txn doesn't keep key at same master
  bool MaybeAbortRemasterTransaction(Transaction* txn);
#endif

  // Place a transaction in a holder if it has keys in this partition
  Transaction* AcceptTransaction(EnvelopePtr&& env);

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
  // Send single-home and lock-only transactions for counter checking
  void SendToRemasterManager(const TxnHolder& txn_holder);
  // Send transactions to lock manager or abort them
  void ProcessRemasterResult(RemasterOccurredResult result);
#endif /* defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY) */

  // Send all transactions for locks, multi-home transactions are only registered
  void SendToLockManager(const TxnHolder& txn_holder);
  void AcquireLocksAndProcessResult(const TxnHolder& txn_holder);

  // Send txn to worker
  void Dispatch(TxnId txn_id);

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
   * Before the transaction data is erased, we wait to collect all lock-onlys of a multi-home txn
   */

  // Start the abort. Only be called once per transaction
  void TriggerPreDispatchAbort(TxnId txn_id);
  // Add a single or multi-home transaction to an abort that started before it
  // arrived
  bool MaybeContinuePreDispatchAbort(TxnId txn_id);
  // Add a lock-only transaction to an abort that started before it arrived
  bool MaybeContinuePreDispatchAbortLockOnly(TxnId txn_id);

  ConfigurationPtr config_;

#if defined(REMASTER_PROTOCOL_SIMPLE)
  SimpleRemasterManager remaster_manager_;
#elif defined(REMASTER_PROTOCOL_PER_KEY)
  PerKeyRemasterManager remaster_manager_;
#endif

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY) || \
    (defined(LOCK_MANAGER_OLD) && !defined(REMASTER_PROTOCOL_COUNTERLESS))
  OldLockManager lock_manager_;
#elif defined(LOCK_MANAGER_DDR)
  DDRLockManager lock_manager_;
#else
  RMALockManager lock_manager_;
#endif

  struct ActiveTxn {
    explicit ActiveTxn(const ConfigurationPtr& config, Transaction* new_txn) : aborting(false), done(false) {
      auto txn_type = new_txn->internal().type();
      if (txn_type == TransactionType::MULTI_HOME || txn_type == TransactionType::LOCK_ONLY) {
        lock_only_txns.resize(new_txn->internal().involved_replicas_size());
      }
      if (txn_type == TransactionType::MULTI_HOME || txn_type == TransactionType::SINGLE_HOME) {
        txn.emplace(config, new_txn);
      } else {
        lock_only_txns[TxnHolder::replica_id(new_txn)].emplace(config, new_txn);
      }
    }

    bool is_ready_for_gc() const {
      bool res = done;
      for (auto& lo : lock_only_txns) {
        res &= lo.has_value();
      }
      return res;
    }

    std::optional<TxnHolder> txn;
    std::vector<std::optional<TxnHolder>> lock_only_txns;
    bool aborting;
    bool done;
  };
  std::unordered_map<TxnId, ActiveTxn> active_txns_;

  TxnHolder& GetTxnHolder(TxnId txn_id);
  TxnHolder& GetLockOnlyTxnHolder(TxnId txn_id, uint32_t rep_id);

  // This must be defined at the end so that the workers exit before any resources
  // in the scheduler is destroyed
  std::vector<unique_ptr<ModuleRunner>> workers_;
};

}  // namespace slog