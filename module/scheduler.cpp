#include "module/scheduler.h"

#include "common/json_utils.h"
#include "common/proto_utils.h"
#include "proto/internal.pb.h"

using std::make_shared;
using std::move;

namespace slog {

using internal::Request;
using internal::Response;

const string Scheduler::WORKERS_ENDPOINT("inproc://workers");
const uint32_t Scheduler::WORKER_LOAD_THRESHOLD = 5;

Scheduler::Scheduler(
    const ConfigurationPtr& config,
    const shared_ptr<Broker>& broker,
    const shared_ptr<Storage<Key, Record>>& storage)
  : // All SINGLE-HOME txn logs take indices in the [0, num_replicas - 1]
    // range, num_replicas can be used as the marker for MULTI-HOME txn log
    kMultiHomeTxnLogMarker(config->GetNumReplicas()),
    config_(config),
    pull_socket_(*broker->GetContext(), ZMQ_PULL),
    worker_socket_(*broker->GetContext(), ZMQ_ROUTER),
    sender_(broker) {
  broker->AddChannel(kSchedulerChannel);
  pull_socket_.bind("inproc://channel_" + kSchedulerChannel);
  pull_socket_.setsockopt(ZMQ_LINGER, 0);
  pull_socket_.setsockopt(ZMQ_RCVHWM, 0);
  poll_items_.push_back({
      static_cast<void*>(pull_socket_),
      0, /* fd */
      ZMQ_POLLIN,
      0 /* revent */});

  worker_socket_.setsockopt(ZMQ_LINGER, 0);
  worker_socket_.setsockopt(ZMQ_RCVHWM, 0);
  worker_socket_.setsockopt(ZMQ_SNDHWM, 0);
  poll_items_.push_back({
      static_cast<void*>(worker_socket_),
      0, /* fd */
      ZMQ_POLLIN,
      0 /* revent */});

  for (size_t i = 0; i < config->GetNumWorkers(); i++) {
    workers_.push_back(MakeRunnerFor<Worker>(
        std::to_string(i), /* identity */
        config,
        *broker->GetContext(),
        storage));
  }

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
  remaster_manager_.SetStorage(storage);
#endif /* defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY) */

}

/***********************************************
                SetUp and Loop
***********************************************/

void Scheduler::SetUp() {
  worker_socket_.bind(WORKERS_ENDPOINT);
  for (auto& worker : workers_) {
    worker->StartInNewThread();
  }
  // Wait for confirmations from all workers
  for (size_t i = 0; i < workers_.size(); i++) {
    MMessage msg(worker_socket_);
    worker_identities_.push_back(msg.GetIdentity());
  }
  next_worker_ = 0;
}

void Scheduler::Loop() {
  zmq::poll(poll_items_, MODULE_POLL_TIMEOUT_MS);

  // Got message from channel
  if (poll_items_[0].revents & ZMQ_POLLIN) {
    MMessage msg(pull_socket_);
    Request req;
    if (msg.GetProto(req)) {
      HandleInternalRequest(move(req));
    }
  }

  // Got messages from workers
  if (poll_items_[1].revents & ZMQ_POLLIN) {
    // Receive from worker socket
    MMessage msg(worker_socket_);
    if (msg.IsProto<Request>()) {
      // A worker wants to send a request to worker on another 
      // machine, so forwarding the request on its behalf
      Request forwarded_req;
      msg.GetProto(forwarded_req);
      string destination;
      // MM_DATA + 1 is a convention between the Scheduler and
      // Worker to specify the destination of this message
      msg.GetString(destination, MM_DATA + 1);
      // Send to the Scheduler of the remote machine
      // TODO: Fix this
      sender_.Send(forwarded_req, kSchedulerChannel, 0);
    } else if (msg.IsProto<Response>()) {
      Response res;
      msg.GetProto(res);
      if (res.type_case() != Response::kWorker) {
        return;
      }
      // Handle the results produced by the worker
      HandleResponseFromWorker(res.worker());
    }
  }

  VLOG_EVERY_N(4, 5000/MODULE_POLL_TIMEOUT_MS) << "Scheduler is alive";
}

/***********************************************
              Internal Requests
***********************************************/

void Scheduler::HandleInternalRequest(Request&& req) {
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

void Scheduler::ProcessRemoteReadResult(
    internal::Request&& req) {
  auto txn_id = req.remote_read_result().txn_id();
  auto& holder = all_txns_[txn_id];
  if (holder.GetTransaction() != nullptr && !holder.GetWorker().empty()) {
    VLOG(2) << "Got remote read result for txn " << txn_id;
    SendToWorker(move(req), holder.GetWorker());
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
    holder.EarlyRemoteReads().emplace_back(move(req));

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
  sender_.Send(res, kServerChannel);
}

/***********************************************
              Worker Responses
***********************************************/

void Scheduler::HandleResponseFromWorker(const internal::WorkerResponse& res) {
  auto txn_id = res.txn_id();
  auto txn_holder = &all_txns_[txn_id];
  auto txn = txn_holder->GetTransaction();

  // Release locks held by this txn. Enqueue the txns that
  // become ready thanks to this release.
  auto unblocked_txns = lock_manager_.ReleaseLocks(all_txns_[txn_id]);
  for (auto unblocked_txn : unblocked_txns) {
    DispatchTransaction(unblocked_txn);
  }

  RecordTxnEvent(
      config_,
      txn->mutable_internal(),
      TransactionEvent::RELEASE_LOCKS);

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
  // If a remaster transaction, trigger any unblocked txns
  if (txn->procedure_case() == Transaction::ProcedureCase::kRemaster) {
    auto& key = txn->write_set().begin()->first;
    auto counter = txn->internal().master_metadata().at(key).counter() + 1;
    ProcessRemasterResult(remaster_manager_.RemasterOccured(key, counter));
  }
#endif /* defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY) */

  SendToCoordinatingServer(txn_id);
  all_txns_.erase(txn_id);
}

void Scheduler::SendToCoordinatingServer(TxnId txn_id) {
  auto& txn_holder = all_txns_[txn_id];
  auto txn = txn_holder.ReleaseTransaction();

  // Send the txn back to the coordinating server
  Request req;
  auto completed_sub_txn = req.mutable_completed_subtxn();
  completed_sub_txn->set_allocated_txn(txn);
  completed_sub_txn->set_partition(config_->GetLocalPartition());
  for (auto p : txn_holder.InvolvedPartitions()) {
    completed_sub_txn->add_involved_partitions(p);
  }

  RecordTxnEvent(
      config_,
      txn->mutable_internal(),
      TransactionEvent::EXIT_SCHEDULER);

  auto& coord_server = txn->internal().coordinating_server();
  auto coordinating_server = config_->MakeMachineIdNum(coord_server.replica(), coord_server.partition());
  sender_.Send(req, kServerChannel, coordinating_server);
  
  txn_holder.SetTransactionNoProcessing(completed_sub_txn->release_txn());
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
      auto txn_replica_id = TransactionHolder::GetTransactionIdReplicaIdPair(txn);
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
      if (holder.KeysInPartition().empty()) {
        all_txns_.erase(txn_id);
        return false;
      }
      break;
    }
    case TransactionType::LOCK_ONLY: {
      auto txn_replica_id = TransactionHolder::GetTransactionIdReplicaIdPair(txn);
      auto& holder = lock_only_txns_[txn_replica_id];

      holder.SetTransaction(config_, txn);
      if (holder.KeysInPartition().empty()) {
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
  auto txn = txn_holder->GetTransaction();
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
    aborting_txn_ids.insert(unblocked_txn_holder->GetTransaction()->internal().id());
  }
  CHECK_EQ(result.should_abort.size(), aborting_txn_ids.size())
      << "Duplicate transactions returned for abort";
  for (auto txn_id : aborting_txn_ids) {
    TriggerPreDispatchAbort(txn_id);
  }
}
#endif /* defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY) */

void Scheduler::SendToLockManager(const TransactionHolder* txn_holder) {
  auto txn_id = txn_holder->GetTransaction()->internal().id();
  auto txn_type = txn_holder->GetTransaction()->internal().type();
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
            txn_holder->GetTransaction()->mutable_internal(),
            TransactionEvent::ACCEPTED);
        DispatchTransaction(txn_id); 
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
  auto txn_id = txn_holder->GetTransaction()->internal().id();

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
  if (lock_manager_.AcquireLocks(*txn_holder)) {
    DispatchTransaction(txn_id);
  }
#else
  switch (lock_manager_.AcquireLocks(*txn_holder)) {
    case AcquireLocksResult::ACQUIRED:
      DispatchTransaction(txn_id);
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
  CHECK(txn_holder.GetWorker() == "")
      << "Dispatched transactions are handled by the worker, txn " << txn_id;

  aborting_txns_.insert(txn_id);

  if (txn_holder.GetTransaction() != nullptr) {
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
  auto txn = txn_holder.GetTransaction();
  auto txn_type = txn->internal().type();
  VLOG(3) << "Main txn of abort arrived: " << txn_id;

  txn->set_status(TransactionStatus::ABORTED);
  SendToCoordinatingServer(txn_id);

  if (txn_holder.InvolvedPartitions().size() > 1) {
    SendAbortToPartitions(txn_id);
  }

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
    DispatchTransaction(unblocked_txn);
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
  auto& involved_replicas = txn_holder.InvolvedReplicas();
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

void Scheduler::SendAbortToPartitions(TxnId txn_id) {
  auto& txn_holder = all_txns_[txn_id];
  Request request;
  auto rrr = request.mutable_remote_read_result();
  rrr->set_txn_id(txn_id);
  rrr->set_partition(config_->GetLocalPartition());
  rrr->set_will_abort(true);
  auto local_replica = config_->GetLocalReplica();
  for (auto p : txn_holder.ActivePartitions()) {
    if (p != config_->GetLocalPartition()) {
      auto machine_id = config_->MakeMachineIdNum(local_replica, p);
      sender_.Send(request, kSchedulerChannel, machine_id);
    }
  }
}

void Scheduler::MaybeFinishAbort(TxnId txn_id) {
  auto& txn_holder = all_txns_[txn_id];
  auto txn = txn_holder.GetTransaction();

  VLOG(3) << "Attempting to finish abort: " << txn_id;

  // Will occur if multiple lock-only's arrive before multi-home
  if (txn == nullptr) {
    return;
  }

  // Active partitions must receive remote reads from all other partitions
  auto num_remote_partitions = txn_holder.InvolvedPartitions().size() - 1;
  auto local_partition = config_->GetLocalPartition();
  auto local_partition_active =
      (txn_holder.ActivePartitions().count(local_partition) > 0);
  if (num_remote_partitions > 0 && local_partition_active) {
    if (txn_holder.EarlyRemoteReads().size() < num_remote_partitions) {
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

void Scheduler::DispatchTransaction(TxnId txn_id) {
  CHECK(all_txns_.count(txn_id) > 0) << "Txn not in all_txns_: " << txn_id;

  auto& txn_holder = all_txns_[txn_id];
  auto txn = txn_holder.GetTransaction();

  // Delete lock-only transactions
  if (txn->internal().type() == TransactionType::MULTI_HOME) {
    for (auto replica : txn_holder.InvolvedReplicas()) {
      lock_only_txns_.erase(std::make_pair(txn->internal().id(), replica));
    }
  }

  // Select next worker in a round-robin manner
  txn_holder.SetWorker(worker_identities_[next_worker_]);
  next_worker_ = (next_worker_ + 1) % worker_identities_.size();

  // Prepare a request with the txn to be sent to the worker
  Request req;
  auto worker_request = req.mutable_worker();
  worker_request->set_txn_holder_ptr(
      reinterpret_cast<uint64_t>(&txn_holder));

  RecordTxnEvent(
      config_,
      txn->mutable_internal(),
      TransactionEvent::DISPATCHED);

  // The transaction need always be sent to a worker before
  // any remote reads is sent for that transaction
  // TODO: pretty sure that the order of messages follow the order of these
  // function calls but should look a bit more into ZMQ ordering to be sure
  SendToWorker(move(req), txn_holder.GetWorker());
  while (!txn_holder.EarlyRemoteReads().empty()) {
    SendToWorker(
        move(txn_holder.EarlyRemoteReads().back()),
        txn_holder.GetWorker());
    txn_holder.EarlyRemoteReads().pop_back();
  }

  VLOG(2) << "Dispatched txn " << txn_id;
}

void Scheduler::SendToWorker(internal::Request&& req, const string& worker) {
  MMessage msg;
  msg.SetIdentity(worker);
  msg.Set(MM_DATA, req);
  msg.SendTo(worker_socket_);
}

} // namespace slog