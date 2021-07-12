#pragma once

#include <glog/logging.h>

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/configuration.h"
#include "common/metrics.h"
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
  Scheduler(const std::shared_ptr<Broker>& broker, const std::shared_ptr<Storage>& storage,
            const MetricsRepositoryManagerPtr& metrics_manager,
            std::chrono::milliseconds poll_timeout = kModuleTimeout);

  std::string name() const override { return "Scheduler"; }

 protected:
  void Initialize() final;

  void OnInternalRequestReceived(EnvelopePtr&& env) final;

  // Handle responses from the workers
  bool OnCustomSocket() final;

 private:
  void ProcessTransaction(EnvelopePtr&& env);
  void ProcessStatsRequest(const internal::StatsRequest& stats_request);

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
  // Send single-home and lock-only transactions for counter checking
  void SendToRemasterManager(Transaction& txn);
  // Send transactions to lock manager or abort them
  void ProcessRemasterResult(RemasterOccurredResult result);
#endif

  // Send all transactions for locks
  void SendToLockManager(Transaction& txn);

  // Send txn to worker
  void Dispatch(TxnId txn_id, bool is_fast);

  /**
   * Aborts
   *
   * Once a transaction is sent to a worker, the worker will manage an abort.
   * If a remaster abort occurs at this partition or if a remote read abort
   * is received before the transaction is dispatched, then the abort is handled
   * here.
   *
   * Before the transaction data is erased, we wait to collect all lock-onlys of a multi-home txn
   */
  void TriggerPreDispatchAbort(TxnId txn_id);

  void MaybeCleanUpTxn(TxnId txn_id);

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

  std::unordered_map<TxnId, TxnHolder> active_txns_;

  // This must be defined at the end so that the workers exit before any resources
  // in the scheduler is destroyed
  std::vector<std::unique_ptr<ModuleRunner>> workers_;

  int64_t global_log_counter_;
};

}  // namespace slog