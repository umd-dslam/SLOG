#include "module/scheduler.h"

#include <glog/logging.h>

#include "common/json_utils.h"
#include "common/proto_utils.h"
#include "common/types.h"
#include "proto/internal.pb.h"

using std::make_shared;
using std::move;

namespace slog {

namespace {
uint32_t SelectWorkerForTxn(TxnId txn_id, uint32_t num_workers) {
  // TODO: Use a hash function
  return txn_id % num_workers;
}
} // namespace

using internal::Request;
using internal::Response;

Scheduler::Scheduler(
    const ConfigurationPtr& config,
    const shared_ptr<Broker>& broker,
    const shared_ptr<Storage<Key, Record>>& storage)
  : NetworkedModule(broker, kSchedulerChannel),
    config_(config) {
  for (size_t i = 0; i < config->num_workers(); i++) {
    workers_.push_back(MakeRunnerFor<Worker>(
        config,
        broker,
        kWorkerChannelOffset + i,
        storage));
  }

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
  remaster_manager_.SetStorage(storage);
#endif /* defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY) */
}

void Scheduler::Initialize() {
  for (auto& worker : workers_) {
    worker->StartInNewThread();
  }
}

/***********************************************
        Internal Requests & Responses
***********************************************/

void Scheduler::HandleInternalRequest(internal::Request&& req, MachineId) {
  switch (req.type_case()) {
    case Request::kForwardTxn: 
      ProcessTransaction(req.mutable_forward_txn()->release_txn());
      break;
    case Request::kRemoteReadResult:
      ProcessRemoteReadResult(move(req));
      break;
    case Request::kStats:
      ProcessStatsRequest(req.stats());
      break;
    default:
      LOG(ERROR) << "Unexpected request type received: \""
                 << CASE_NAME(req.type_case(), Request) << "\"";
      break;
  }
}

void Scheduler::ProcessRemoteReadResult(internal::Request&& req) {
  auto txn_id = req.remote_read_result().txn_id();
  auto& holder = all_txns_[txn_id];
  if (holder.transaction() != nullptr && holder.worker()) {
    VLOG(2) << "Got remote read result for txn " << txn_id;
    Send(move(req), kWorkerChannelOffset + *holder.worker());
  } else {
    // Save the remote reads that come before the txn
    // is processed by this partition
    //
    // NOTE: The logic guarantees that it'd never happens but if somehow this
    // request was not needed but still arrived AFTER the transaction 
    // was already commited, it would be stuck in early_remote_reads forever.
    // Consider garbage collecting them if that happens.
    VLOG(2) << "Got early remote read result for txn " << txn_id;

    auto remote_abort = req.remote_read_result().will_abort();
    holder.early_remote_reads().emplace_back(move(req));

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
  stats.AddMember(StringRef(NUM_ALL_TXNS), all_txns_.size(), alloc);
  if (level >= 1) {
    stats.AddMember(
        StringRef(ALL_TXNS),
        ToJsonArray(all_txns_, [](const auto& p) { return p.first; }, alloc),
        alloc);
  }

  // Add stats from the lock manager
  lock_manager_.GetStats(stats, level);

  // Write JSON object to a buffer and send back to the server
  rapidjson::StringBuffer buf;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
  stats.Accept(writer);

  internal::Response res;
  res.mutable_stats()->set_id(stats_request.id());
  res.mutable_stats()->set_stats_json(buf.GetString());
  Send(res, kServerChannel);
}

void Scheduler::HandleInternalResponse(internal::Response&& res, MachineId) {
  auto txn_id = res.worker().txn_id();
  auto txn_holder = &all_txns_[txn_id];
  auto txn = txn_holder->transaction();

  // Release locks held by this txn. Enqueue the txns that
  // become ready thanks to this release.
  auto unblocked_txns = lock_manager_.ReleaseLocks(all_txns_[txn_id]);
  for (auto unblocked_txn : unblocked_txns) {
    Dispatch(unblocked_txn);
  }

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
  // If a remaster transaction, trigger any unblocked txns
  if (txn->procedure_case() == Transaction::ProcedureCase::kRemaster) {
    auto& key = txn->write_set().begin()->first;
    auto counter = txn->internal().master_metadata().at(key).counter() + 1;
    ProcessRemasterResult(remaster_manager_.RemasterOccured(key, counter));
  }
#endif /* defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY) */

  all_txns_.erase(txn_id);
}

/***********************************************
              Transaction Processing
***********************************************/

void Scheduler::ProcessTransaction(Transaction* txn) {
  auto txn_internal = txn->mutable_internal();
  
  RecordTxnEvent(
      config_,
      txn_internal,
      TransactionEvent::ENTER_SCHEDULER);

  if (!AcceptTransaction(txn)) {
    return;
  }

  auto txn_id = txn_internal->id();
  auto txn_type = txn_internal->type();
  switch (txn_type) {
    case TransactionType::SINGLE_HOME: {
      VLOG(2) << "Accepted SINGLE-HOME transaction " << txn_id;

      if (MaybeContinuePreDispatchAbort(txn_id)) {
        break;
      }

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
      if (txn->procedure_case() == Transaction::ProcedureCase::kRemaster) {
        if (MaybeAbortRemasterTransaction(txn)) {
          break;
        }
      }
      SendToRemasterManager(&all_txns_[txn_id]);
#else
      SendToLockManager(&all_txns_[txn_id]);
#endif
      break;
    }
    case TransactionType::LOCK_ONLY: {
      auto txn_replica_id = TransactionHolder::transaction_id_replica_id(txn);
      VLOG(2) << "Accepted LOCK-ONLY transaction "
          << txn_replica_id.first <<", " << txn_replica_id.second;

      if (MaybeContinuePreDispatchAbortLockOnly(txn_replica_id)) {
        break;
      }

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
      SendToRemasterManager(&lock_only_txns_[txn_replica_id]);
#else
      SendToLockManager(&lock_only_txns_[txn_replica_id]);
#endif

      break;
    }
    case TransactionType::MULTI_HOME: {
      VLOG(2) << "Accepted MULTI-HOME transaction " << txn_id;
      auto txn_holder = &all_txns_[txn_id];

      if (aborting_txns_.count(txn_id)) {
        MaybeContinuePreDispatchAbort(txn_id);
        break;
      }

#ifdef REMASTER_PROTOCOL_COUNTERLESS
      if (txn->procedure_case() == Transaction::ProcedureCase::kRemaster) {
        if (MaybeAbortRemasterTransaction(txn)) {
          break;
        }
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

bool Scheduler::AcceptTransaction(Transaction* txn) {
  switch(txn->internal().type()) {
    case TransactionType::SINGLE_HOME:
    case TransactionType::MULTI_HOME: {
      auto txn_id = txn->internal().id();
      auto& holder = all_txns_[txn_id];
      
      holder.SetTransaction(config_, txn);
      if (holder.keys_in_partition().empty()) {
        all_txns_.erase(txn_id);
        return false;
      }
      break;
    }
    case TransactionType::LOCK_ONLY: {
      auto txn_replica_id = TransactionHolder::transaction_id_replica_id(txn);
      auto& holder = lock_only_txns_[txn_replica_id];

      holder.SetTransaction(config_, txn);
      if (holder.keys_in_partition().empty()) {
        lock_only_txns_.erase(txn_replica_id);
        return false;
      }
      break;
    }
    default:
      LOG(ERROR) << "Unknown transaction type";
      break;
  }
  return true;
}

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
void Scheduler::SendToRemasterManager(TransactionHolder* txn_holder) {
  auto txn = txn_holder->transaction();
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
    SendToLockManager(unblocked_txn_holder);
  }
  // Check for duplicates
  // TODO: remove this set and check
  unordered_set<TxnId> aborting_txn_ids;
  for (auto unblocked_txn_holder : result.should_abort) {
    aborting_txn_ids.insert(unblocked_txn_holder->transaction()->internal().id());
  }
  CHECK_EQ(result.should_abort.size(), aborting_txn_ids.size())
      << "Duplicate transactions returned for abort";
  for (auto txn_id : aborting_txn_ids) {
    TriggerPreDispatchAbort(txn_id);
  }
}
#endif /* defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY) */

void Scheduler::SendToLockManager(const TransactionHolder* txn_holder) {
  auto txn_id = txn_holder->transaction()->internal().id();
  auto txn_type = txn_holder->transaction()->internal().type();
  switch(txn_type) {
    case TransactionType::SINGLE_HOME: {
      lock_manager_.AcceptTransaction(*txn_holder);
      AcquireLocksAndProcessResult(txn_holder);
      break;
    }
    case TransactionType::MULTI_HOME: {
      if (lock_manager_.AcceptTransaction(*txn_holder)) {
        // Note: this only records when MH arrives after lock-onlys
        RecordTxnEvent(
            config_,
            txn_holder->transaction()->mutable_internal(),
            TransactionEvent::ACCEPTED);
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

void Scheduler::AcquireLocksAndProcessResult(const TransactionHolder* txn_holder) {
  auto txn_id = txn_holder->transaction()->internal().id();

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
  if (lock_manager_.AcquireLocks(*txn_holder)) {
    Dispatch(txn_id);
  }
#else
  switch (lock_manager_.AcquireLocks(*txn_holder)) {
    case AcquireLocksResult::ACQUIRED:
      Dispatch(txn_id);
      break;
    case AcquireLocksResult::ABORT:
      TriggerPreDispatchAbort(txn_id);
      break;
    case AcquireLocksResult::WAITING:
      break;
    default:
      LOG(ERROR) << "Unknown lock result type";
      break;
  }
#endif /* defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY) */

}

/***********************************************
         Pre-Dispatch Abort Processing
***********************************************/

void Scheduler::TriggerPreDispatchAbort(TxnId txn_id) {
  CHECK(aborting_txns_.count(txn_id) == 0) << "Abort was triggered twice: " << txn_id;
  VLOG(2) << "Triggering abort of txn: " << txn_id;

  auto& txn_holder = all_txns_[txn_id];
  CHECK(!txn_holder.worker())
      << "Dispatched transactions are handled by the worker, txn " << txn_id;

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

  auto& txn_holder = all_txns_[txn_id];
  auto txn = txn_holder.transaction();
  auto txn_type = txn->internal().type();
  VLOG(3) << "Main txn of abort arrived: " << txn_id;

  // Let a worker handle notifying other partitions and
  // send back to the server. Use one-way dispatching to avoid
  // race condition between the scheduler and the worker.
  txn->set_status(TransactionStatus::ABORTED);
  Dispatch(txn_id, /* one-way */ true);

  // Release txn from remaster manager and lock manager.
  //
  // If the abort was triggered by a remote partition,
  // then the single-home or multi-home transaction may still
  // be in one of the managers, and needs to be removed.
  //
  // This also releases any lock-only transactions.
#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
  ProcessRemasterResult(
    remaster_manager_.ReleaseTransaction(&txn_holder));
#endif /* defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY) */

  // Release locks held by this txn. Enqueue the txns that
  // become ready thanks to this release.
  auto unblocked_txns = lock_manager_.ReleaseLocks(all_txns_[txn_id]);
  for (auto unblocked_txn : unblocked_txns) {
    Dispatch(unblocked_txn);
  }

  if (txn_type == TransactionType::MULTI_HOME) {
    CollectLockOnlyTransactionsForAbort(txn_id);
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

void Scheduler::CollectLockOnlyTransactionsForAbort(TxnId txn_id) {
  auto& txn_holder = all_txns_[txn_id];
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

void Scheduler::MaybeFinishAbort(TxnId txn_id) {
  auto& txn_holder = all_txns_[txn_id];
  auto txn = txn_holder.transaction();

  VLOG(3) << "Attempting to finish abort: " << txn_id;

  // Will occur if multiple lock-only's arrive before multi-home
  if (txn == nullptr) {
    return;
  }

  // Active partitions must receive remote reads from all other partitions
  auto num_remote_partitions = txn_holder.involved_partitions().size() - 1;
  auto local_partition = config_->local_partition();
  auto local_partition_active =
      (txn_holder.active_partitions().count(local_partition) > 0);
  if (num_remote_partitions > 0 && local_partition_active) {
    if (txn_holder.early_remote_reads().size() < num_remote_partitions) {
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
  all_txns_.erase(txn_id);

  VLOG(3) << "Finished abort: " << txn_id;
}

/***********************************************
              Transaction Dispatch
***********************************************/

void Scheduler::Dispatch(TxnId txn_id, bool one_way) {
  CHECK(all_txns_.count(txn_id) > 0) << "Txn not in all_txns_: " << txn_id;

  TransactionHolder* txn_holder;
  if (one_way) {
    txn_holder = new TransactionHolder(all_txns_[txn_id]);
  } else {
    txn_holder = &all_txns_[txn_id];
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
  txn_holder->SetWorker(SelectWorkerForTxn(txn_id, config_->num_workers()));

  RecordTxnEvent(
      config_,
      txn->mutable_internal(),
      TransactionEvent::DISPATCHED);

  // Prepare a request with the txn to be sent to the worker
  Request req;
  auto worker_request = req.mutable_worker();
  worker_request->set_txn_holder_ptr(reinterpret_cast<uint64_t>(txn_holder));
  Channel worker_channel = kWorkerChannelOffset + *txn_holder->worker();

  // The transaction need always be sent to a worker before
  // any remote reads is sent for that transaction
  Send(move(req), worker_channel);
  while (!txn_holder->early_remote_reads().empty()) {
    Send(move(txn_holder->early_remote_reads().back()), worker_channel);
    txn_holder->early_remote_reads().pop_back();
  }

  VLOG(2) << "Dispatched txn " << txn_id;
}

} // namespace slog