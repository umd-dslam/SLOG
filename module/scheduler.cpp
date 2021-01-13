#include "module/scheduler.h"

#include <glog/logging.h>

#include <algorithm>

#include "common/json_utils.h"
#include "common/monitor.h"
#include "common/proto_utils.h"
#include "common/types.h"
#include "proto/internal.pb.h"

using std::make_shared;
using std::move;

namespace slog {

using internal::Request;
using internal::Response;

Scheduler::Scheduler(const ConfigurationPtr& config, const shared_ptr<Broker>& broker,
                     const shared_ptr<Storage<Key, Record>>& storage, std::chrono::milliseconds poll_timeout)
    : NetworkedModule("Scheduler", broker, kSchedulerChannel, poll_timeout), config_(config), current_worker_(0) {
  for (size_t i = 0; i < config->num_workers(); i++) {
    workers_.push_back(MakeRunnerFor<Worker>(config, broker, kWorkerChannelOffset + i, storage, poll_timeout));
  }

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
  remaster_manager_.SetStorage(storage);
#endif /* defined(REMASTER_PROTOCOL_SIMPLE) || \
          defined(REMASTER_PROTOCOL_PER_KEY) */
}

void Scheduler::Initialize() {
  for (auto& worker : workers_) {
    worker->StartInNewThread();
  }
}

/***********************************************
        Internal Requests & Responses
***********************************************/

void Scheduler::HandleInternalRequest(EnvelopePtr&& env) {
  switch (env->request().type_case()) {
    case Request::kForwardTxn:
      ProcessTransaction(move(env));
      break;
    case Request::kRemoteReadResult:
      ProcessRemoteReadResult(move(env));
      break;
    case Request::kStats:
      ProcessStatsRequest(env->request().stats());
      break;
    default:
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(env->request().type_case(), Request) << "\"";
      break;
  }
}

void Scheduler::ProcessRemoteReadResult(EnvelopePtr&& env) {
  auto txn_id = env->request().remote_read_result().txn_id();
  auto& txn_info = active_txns_[txn_id];
  if (txn_info.holder.has_value() && txn_info.holder->worker().has_value()) {
    VLOG(2) << "Got remote read result for txn " << txn_id;
    Send(move(env), kWorkerChannelOffset + txn_info.holder->worker().value());
  } else {
    // Save the remote reads that come before the txn
    // is processed by this partition
    //
    // NOTE: The logic guarantees that it'd never happens but if somehow this
    // request was not needed but still arrived AFTER the transaction
    // was already commited, it would be stuck in early_remote_reads forever.
    // Consider garbage collecting them if that happens.
    VLOG(2) << "Got early remote read result for txn " << txn_id;

    auto remote_abort = env->request().remote_read_result().will_abort();
    txn_info.early_remote_reads.emplace_back(move(env));

    if (aborting_txns_.count(txn_id) > 0) {
      // Check if this is the last required remote read
      MaybeFinishAbort(txn_id);
    } else if (remote_abort) {
      TriggerPreDispatchAbort(txn_id);
    }
  }
}

void Scheduler::ProcessStatsRequest(const internal::StatsRequest& stats_request) {
  using rapidjson::StringRef;

  int level = stats_request.level();

  rapidjson::Document stats;
  stats.SetObject();
  auto& alloc = stats.GetAllocator();

  // Add stats for current transactions in the system
  stats.AddMember(StringRef(NUM_ALL_TXNS), active_txns_.size(), alloc);
  if (level >= 1) {
    stats.AddMember(StringRef(ALL_TXNS),
                    ToJsonArray(
                        active_txns_, [](const auto& p) { return p.first; }, alloc),
                    alloc);
  }

  // Add stats from the lock manager
  lock_manager_.GetStats(stats, level);

  // Write JSON object to a buffer and send back to the server
  rapidjson::StringBuffer buf;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
  stats.Accept(writer);

  auto env = NewEnvelope();
  env->mutable_response()->mutable_stats()->set_id(stats_request.id());
  env->mutable_response()->mutable_stats()->set_stats_json(buf.GetString());
  Send(move(env), kServerChannel);
}

void Scheduler::HandleInternalResponse(EnvelopePtr&& env) {
  auto txn_id = env->response().worker().txn_id();
  auto txn_info_it = active_txns_.find(txn_id);

  DCHECK(txn_info_it != active_txns_.end());

  auto& txn_holder = txn_info_it->second.holder.value();
  // Release locks held by this txn. Enqueue the txns that
  // become ready thanks to this release.
  auto unblocked_txns = lock_manager_.ReleaseLocks(txn_holder);
  for (auto unblocked_txn : unblocked_txns) {
    Dispatch(unblocked_txn);
  }

  VLOG(2) << "Released locks of txn " << txn_id;

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
  auto txn = txn_holder.transaction();
  // If a remaster transaction, trigger any unblocked txns
  if (txn->procedure_case() == Transaction::ProcedureCase::kRemaster) {
    auto& key = txn->write_set().begin()->first;
    auto counter = txn->internal().master_metadata().at(key).counter() + 1;
    ProcessRemasterResult(remaster_manager_.RemasterOccured(key, counter));
  }
#endif /* defined(REMASTER_PROTOCOL_SIMPLE) || \
          defined(REMASTER_PROTOCOL_PER_KEY) */

  active_txns_.erase(txn_id);
}

/***********************************************
              Transaction Processing
***********************************************/

void Scheduler::ProcessTransaction(EnvelopePtr&& env) {
  auto txn = AcceptTransaction(move(env));
  if (txn == nullptr) {
    return;
  }

  TRACE(txn->mutable_internal(), TransactionEvent::ENTER_SCHEDULER);

  auto txn_id = txn->internal().id();
  auto txn_type = txn->internal().type();
  switch (txn_type) {
    case TransactionType::SINGLE_HOME: {
      VLOG(2) << "Accepted SINGLE-HOME transaction " << txn_id;

      if (MaybeContinuePreDispatchAbort(txn_id)) {
        break;
      }

      auto txn_info_it = active_txns_.find(txn_id);
      DCHECK(txn_info_it != active_txns_.end());
      DCHECK(txn_info_it->second.holder.has_value());

      auto& txn_holder = txn_info_it->second.holder.value();

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
      if (txn->procedure_case() == Transaction::ProcedureCase::kRemaster) {
        if (MaybeAbortRemasterTransaction(txn)) {
          break;
        }
      }
      SendToRemasterManager(txn_holder);
#else
      SendToLockManager(txn_holder);
#endif
      break;
    }
    case TransactionType::LOCK_ONLY: {
      auto txn_replica_id = TxnHolder::transaction_id_replica_id(txn);
      VLOG(2) << "Accepted LOCK-ONLY transaction " << txn_replica_id.first << ", home = " << txn_replica_id.second;

      if (MaybeContinuePreDispatchAbortLockOnly(txn_replica_id)) {
        break;
      }

      auto txn_holder_it = lock_only_txns_.find(txn_replica_id);
      DCHECK(txn_holder_it != lock_only_txns_.end());

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
      SendToRemasterManager(txn_holder_it->second);
#else
      SendToLockManager(txn_holder_it->second);
#endif

      break;
    }
    case TransactionType::MULTI_HOME: {
      VLOG(2) << "Accepted MULTI-HOME transaction " << txn_id;

      auto txn_info_it = active_txns_.find(txn_id);
      DCHECK(txn_info_it != active_txns_.end());
      DCHECK(txn_info_it->second.holder.has_value());

      auto& txn_holder = txn_info_it->second.holder.value();

      if (aborting_txns_.count(txn_id)) {
        MaybeContinuePreDispatchAbort(txn_id);
        break;
      }

#ifdef REMASTER_PROTOCOL_COUNTERLESS
      if (txn->procedure_case() == Transaction::ProcedureCase::kRemaster && MaybeAbortRemasterTransaction(txn)) {
        break;
      }
#endif

      SendToLockManager(txn_holder);
      break;
    }
    default:
      LOG(ERROR) << "Unknown transaction type";
      break;
  }
}

#ifdef ENABLE_REMASTER
bool Scheduler::MaybeAbortRemasterTransaction(Transaction* txn) {
  // TODO: this check can be done as soon as metadata is assigned
  auto txn_id = txn->internal().id();
  auto past_master = txn->internal().master_metadata().begin()->second.master();
  if (txn->remaster().new_master() == past_master) {
    TriggerPreDispatchAbort(txn_id);
    return true;
  }
  return false;
}
#endif

Transaction* Scheduler::AcceptTransaction(EnvelopePtr&& env) {
  auto txn = env->mutable_request()->mutable_forward_txn()->release_txn();
  switch (txn->internal().type()) {
    case TransactionType::SINGLE_HOME:
    case TransactionType::MULTI_HOME: {
      auto txn_id = txn->internal().id();
      auto& txn_info = active_txns_[txn_id];
      txn_info.holder.emplace(config_, txn);
      if (txn_info.holder->keys_in_partition().empty()) {
        active_txns_.erase(txn_id);
        return nullptr;
      }
      break;
    }
    case TransactionType::LOCK_ONLY: {
      auto txn_replica_id = TxnHolder::transaction_id_replica_id(txn);
      auto ins = lock_only_txns_.try_emplace(txn_replica_id, config_, txn);
      if (!ins.second) {
        LOG(ERROR) << "Already received LOCK-ONLY txn: " << txn_replica_id.first;
        return nullptr;
      }

      auto& txn_holder = ins.first->second;
      if (txn_holder.keys_in_partition().empty()) {
        lock_only_txns_.erase(ins.first);
        return nullptr;
      }
      break;
    }
    default:
      LOG(ERROR) << "Unknown transaction type";
      return nullptr;
  }
  return txn;
}

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
void Scheduler::SendToRemasterManager(const TxnHolder& txn_holder) {
  auto txn = txn_holder.transaction();
  auto txn_type = txn->internal().type();
  CHECK(txn_type == TransactionType::SINGLE_HOME || txn_type == TransactionType::LOCK_ONLY)
      << "MH aren't sent to the remaster manager";

  switch (remaster_manager_.VerifyMaster(txn_holder)) {
    case VerifyMasterResult::VALID: {
      SendToLockManager(txn_holder);
      break;
    }
    case VerifyMasterResult::ABORT: {
      TriggerPreDispatchAbort(txn->internal().id());
      break;
    }
    case VerifyMasterResult::WAITING: {
      VLOG(4) << "Txn waiting on remaster: " << txn->internal().id();
      // Do nothing
      break;
    }
    default:
      LOG(ERROR) << "Unknown VerifyMaster type";
      break;
  }
}

void Scheduler::ProcessRemasterResult(RemasterOccurredResult result) {
  for (auto unblocked_txn_holder : result.unblocked) {
    SendToLockManager(*unblocked_txn_holder);
  }
  // Check for duplicates
  // TODO: remove this set and check
  unordered_set<TxnId> aborting_txn_ids;
  for (auto unblocked_txn_holder : result.should_abort) {
    aborting_txn_ids.insert(unblocked_txn_holder->transaction()->internal().id());
  }
  CHECK_EQ(result.should_abort.size(), aborting_txn_ids.size()) << "Duplicate transactions returned for abort";
  for (auto txn_id : aborting_txn_ids) {
    TriggerPreDispatchAbort(txn_id);
  }
}
#endif /* defined(REMASTER_PROTOCOL_SIMPLE) || \
          defined(REMASTER_PROTOCOL_PER_KEY) */

void Scheduler::SendToLockManager(const TxnHolder& txn_holder) {
  auto txn_id = txn_holder.transaction()->internal().id();
  auto txn_type = txn_holder.transaction()->internal().type();
  switch (txn_type) {
    case TransactionType::SINGLE_HOME: {
      lock_manager_.AcceptTransaction(txn_holder);
      AcquireLocksAndProcessResult(txn_holder);
      break;
    }
    case TransactionType::MULTI_HOME: {
      if (lock_manager_.AcceptTransaction(txn_holder)) {
        // Note: this only records when MH arrives after lock-onlys
        TRACE(txn_holder.transaction()->mutable_internal(), TransactionEvent::ACCEPTED);

        Dispatch(txn_id);
      }
      break;
    }
    case TransactionType::LOCK_ONLY: {
      AcquireLocksAndProcessResult(txn_holder);
      break;
    }
    default:
      LOG(ERROR) << "Unknown transaction type";
      break;
  }
}

void Scheduler::AcquireLocksAndProcessResult(const TxnHolder& txn_holder) {
  auto txn_id = txn_holder.transaction()->internal().id();
  VLOG(2) << "Trying to acquires locks of txn " << txn_id;
  switch (lock_manager_.AcquireLocks(txn_holder)) {
    case AcquireLocksResult::ACQUIRED:
      Dispatch(txn_id);
      break;
    case AcquireLocksResult::ABORT:
      TriggerPreDispatchAbort(txn_id);
      break;
    case AcquireLocksResult::WAITING:
      VLOG(2) << "Txn " << txn_id << " cannot be dispatched yet";
      break;
    default:
      LOG(ERROR) << "Unknown lock result type";
      break;
  }
}

/***********************************************
         Pre-Dispatch Abort Processing
***********************************************/

void Scheduler::TriggerPreDispatchAbort(TxnId txn_id) {
  DCHECK(aborting_txns_.count(txn_id) == 0) << "Abort was triggered twice: " << txn_id;
  VLOG(2) << "Triggering pre-dispatch abort of txn " << txn_id;

  auto it = active_txns_.find(txn_id);
  DCHECK(it != active_txns_.end());
  DCHECK(it->second.holder.has_value());

  auto& txn_holder = it->second.holder.value();
  CHECK(!txn_holder.worker()) << "Dispatched transactions are handled by a worker, txn " << txn_id;

  aborting_txns_.insert(txn_id);

  if (txn_holder.transaction() != nullptr) {
    MaybeContinuePreDispatchAbort(txn_id);
  } else {
    VLOG(3) << "Defering abort until txn arrives: " << txn_id;
  }
}

bool Scheduler::MaybeContinuePreDispatchAbort(TxnId txn_id) {
  if (aborting_txns_.count(txn_id) == 0) {
    return false;
  }

  auto it = active_txns_.find(txn_id);
  DCHECK(it != active_txns_.end());
  DCHECK(it->second.holder.has_value());

  auto& txn_holder = it->second.holder.value();

  auto txn = txn_holder.transaction();
  auto txn_type = txn->internal().type();
  VLOG(3) << "Main txn of abort arrived: " << txn_id;

  // Let a worker handle notifying other partitions and
  // send back to the server. Use one-way dispatching to avoid
  // race condition between the scheduler and the worker.
  if (txn->status() != TransactionStatus::ABORTED) {
    txn->set_status(TransactionStatus::ABORTED);
    Dispatch(txn_id, /* one-way */ true);
  }

  // Release txn from remaster manager and lock manager.
  //
  // If the abort was triggered by a remote partition,
  // then the single-home or multi-home transaction may still
  // be in one of the managers, and needs to be removed.
  //
  // This also releases any lock-only transactions.
#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
  ProcessRemasterResult(remaster_manager_.ReleaseTransaction(txn_holder));
#endif /* defined(REMASTER_PROTOCOL_SIMPLE) || \
          defined(REMASTER_PROTOCOL_PER_KEY) */

  // Release locks held by this txn. Enqueue the txns that
  // become ready thanks to this release.
  auto unblocked_txns = lock_manager_.ReleaseLocks(txn_holder);
  for (auto unblocked_txn : unblocked_txns) {
    Dispatch(unblocked_txn);
  }

  // Collect lock only txn for abort
  if (txn_type == TransactionType::MULTI_HOME) {
    auto& involved_replicas = txn_holder.involved_replicas();
    mh_abort_waiting_on_[txn_id] += involved_replicas.size();

    // Erase the LOs that have already arrived - the same that have been released
    // from remaster and lock managers
    for (auto replica : involved_replicas) {
      auto txn_replica_id = std::make_pair(txn_id, replica);
      if (lock_only_txns_.count(txn_replica_id)) {
        lock_only_txns_.erase(txn_replica_id);
        mh_abort_waiting_on_[txn_id] -= 1;
      }
    }
  }

  MaybeFinishAbort(txn_id);
  return true;
}

bool Scheduler::MaybeContinuePreDispatchAbortLockOnly(TxnIdReplicaIdPair txn_replica_id) {
  auto txn_id = txn_replica_id.first;
  if (aborting_txns_.count(txn_id) == 0) {
    return false;
  }
  VLOG(3) << "Aborting lo txn arrived: " << txn_id << ", " << txn_replica_id.second;
  lock_only_txns_.erase(txn_replica_id);
  mh_abort_waiting_on_[txn_id] -= 1;

  // Check if this was the last lock-only
  MaybeFinishAbort(txn_id);
  return true;
}

void Scheduler::MaybeFinishAbort(TxnId txn_id) {
  auto it = active_txns_.find(txn_id);
  DCHECK(it != active_txns_.end());
  DCHECK(it->second.holder.has_value());
  auto& txn_holder = it->second.holder.value();
  auto& early_remote_reads = it->second.early_remote_reads;
  auto txn = txn_holder.transaction();

  VLOG(3) << "Attempting to finish abort: " << txn_id;

  // Will occur if multiple lock-only's arrive before multi-home
  if (txn == nullptr) {
    return;
  }

  // Active partitions must receive remote reads from all other partitions
  auto num_remote_partitions = txn->internal().involved_partitions_size() - 1;
  auto local_partition = config_->local_partition();
  auto local_partition_active = std::find(txn_holder.active_partitions().begin(), txn_holder.active_partitions().end(),
                                          local_partition) != txn_holder.active_partitions().end();
  if (num_remote_partitions > 0 && local_partition_active) {
    if (early_remote_reads.size() < num_remote_partitions) {
      return;
    }
  }

  // Multi-homes must collect all lock-onlys
  auto txn_type = txn->internal().type();
  if (txn_type == TransactionType::MULTI_HOME) {
    if (mh_abort_waiting_on_[txn_id] != 0) {
      return;
    } else {
      mh_abort_waiting_on_.erase(txn_id);
    }
  }

  aborting_txns_.erase(txn_id);
  active_txns_.erase(txn_id);

  VLOG(3) << "Finished abort: " << txn_id;
}

/***********************************************
              Transaction Dispatch
***********************************************/

void Scheduler::Dispatch(TxnId txn_id, bool one_way) {
  auto it = active_txns_.find(txn_id);
  DCHECK(it != active_txns_.end()) << "Cannot dispatch unactive txn: " << txn_id;
  DCHECK(it->second.holder.has_value());

  TxnHolder* txn_holder;
  if (one_way) {
    txn_holder = new TxnHolder(it->second.holder.value());
  } else {
    txn_holder = &it->second.holder.value();
  }

  auto txn = txn_holder->transaction();

  // Delete lock-only transactions
  // TODO: Move this code out of this function since it causes a side effect
  //       Only execise this code if this is not a one-way dispatch for now.
  if (!one_way && txn->internal().type() == TransactionType::MULTI_HOME) {
    for (auto replica : txn_holder->involved_replicas()) {
      lock_only_txns_.erase(std::make_pair(txn->internal().id(), replica));
    }
  }

  // Select a worker for this transaction
  txn_holder->SetWorker(current_worker_);
  current_worker_ = (current_worker_ + 1) % workers_.size();

  TRACE(txn->mutable_internal(), TransactionEvent::DISPATCHED);

  // Prepare a request with the txn to be sent to the worker
  auto env = NewEnvelope();
  auto worker_request = env->mutable_request()->mutable_worker();
  worker_request->set_txn_holder_ptr(reinterpret_cast<uint64_t>(txn_holder));
  Channel worker_channel = kWorkerChannelOffset + *txn_holder->worker();

  // The transaction need always be sent to a worker before
  // any remote reads is sent for that transaction
  auto& early_remote_reads = it->second.early_remote_reads;
  Send(move(env), worker_channel);
  while (!early_remote_reads.empty()) {
    Send(move(early_remote_reads.back()), worker_channel);
    early_remote_reads.pop_back();
  }

  VLOG(2) << "Dispatched txn " << txn_id;
}

}  // namespace slog