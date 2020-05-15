#include "module/scheduler.h"

#include <stack>

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
    ConfigurationPtr config,
    zmq::context_t& context,
    Broker& broker,
    shared_ptr<Storage<Key, Record>> storage)
  : ChannelHolder(broker.AddChannel(SCHEDULER_CHANNEL)),
    // All SINGLE-HOME txn logs take indices in the [0, num_replicas - 1]
    // range, num_replicas can be used as the marker for MULTI-HOME txn log
    kMultiHomeTxnLogMarker(config->GetNumReplicas()),
    config_(config),
    worker_socket_(context, ZMQ_ROUTER),
    remaster_manager_(storage) {
  worker_socket_.setsockopt(ZMQ_LINGER, 0);
  worker_socket_.setsockopt(ZMQ_RCVHWM, 0);
  worker_socket_.setsockopt(ZMQ_SNDHWM, 0);
  poll_items_.push_back(GetChannelPollItem());
  poll_items_.push_back({
      static_cast<void*>(worker_socket_),
      0, /* fd */
      ZMQ_POLLIN,
      0 /* revent */});

  for (size_t i = 0; i < config->GetNumWorkers(); i++) {
    workers_.push_back(MakeRunnerFor<Worker>(
        std::to_string(i), /* identity */
        config,
        context,
        storage));
  }
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

  if (HasMessageFromChannel()) {
    MMessage msg;
    ReceiveFromChannel(msg);
    Request req;
    if (msg.GetProto(req)) {
      HandleInternalRequest(move(req), msg.GetIdentity());
    }
  }

  if (HasMessageFromWorker()) {
    // Receive from worker socket
    MMessage msg(worker_socket_);
    if (msg.IsProto<Request>()) {
      // A worker wants to send a request to worker on another 
      // machine, so forwarding the request on its behalf
      Request forwarded_req;
      msg.GetProto(forwarded_req);
      string destination;
      // MM_PROTO + 1 is a convention between the Scheduler and
      // Worker to specify the destination of this message
      msg.GetString(destination, MM_PROTO + 1);
      // Send to the Scheduler of the remote machine
      SendSameChannel(forwarded_req, destination);
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

bool Scheduler::HasMessageFromChannel() const {
  return poll_items_[0].revents & ZMQ_POLLIN;
}

bool Scheduler::HasMessageFromWorker() const {
  return poll_items_[1].revents & ZMQ_POLLIN;
}

/***********************************************
              Internal Requests
***********************************************/

void Scheduler::HandleInternalRequest(
    Request&& req,
    const string& from_machine_id) {
  switch (req.type_case()) {
    case Request::kForwardBatch: 
      ProcessForwardBatch(
          req.mutable_forward_batch(), from_machine_id);
      break;
    case Request::kLocalQueueOrder:
      ProcessLocalQueueOrder(req.local_queue_order());
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
  MaybeUpdateLocalLog();
  MaybeProcessNextBatchesFromGlobalLog();
}

void Scheduler::ProcessForwardBatch(
    internal::ForwardBatch* forward_batch,
    const string& from_machine_id) {
  auto machine_id = from_machine_id.empty() 
      ? config_->GetLocalMachineIdAsProto() 
      : MakeMachineId(from_machine_id);
  auto from_replica = machine_id.replica();

  switch (forward_batch->part_case()) {
    case internal::ForwardBatch::kBatchData: {
      auto batch = BatchPtr(forward_batch->release_batch_data());
      RecordTxnEvent(
          config_,
          batch.get(),
          TransactionEvent::ENTER_SCHEDULER_IN_BATCH);

      switch (batch->transaction_type()) {
        case TransactionType::SINGLE_HOME: {
          VLOG(1) << "Received data for SINGLE-HOME batch " << batch->id()
              << " from [" << from_machine_id
              << "]. Number of txns: " << batch->transactions_size();
          // If this batch comes from the local region, put it into the local interleaver
          if (from_replica == config_->GetLocalReplica()) {
            local_interleaver_.AddBatchId(
                machine_id.partition() /* queue_id */,
                // Batches generated by the same machine need to follow the order
                // of creation. This field is used to keep track of that order
                forward_batch->same_origin_position(),
                batch->id());
          }
          
          all_logs_[from_replica].AddBatch(move(batch));
          break;
        }
        case TransactionType::MULTI_HOME: {
          VLOG(1) << "Received data for MULTI-HOME batch " << batch->id()
              << " from [" << from_machine_id
              << "]. Number of txns: " << batch->transactions_size();
          // MULTI-HOME txns are already ordered with respect to each other
          // and their IDs have been replaced with slot id in the orderer module
          // so here their id and slot id are the same
          all_logs_[kMultiHomeTxnLogMarker].AddSlot(batch->id(), batch->id());
          all_logs_[kMultiHomeTxnLogMarker].AddBatch(move(batch));
          break;
        }
        default:
          LOG(ERROR) << "Received batch with invalid transaction type. "
                     << "Only SINGLE_HOME and MULTI_HOME are accepted. Received "
                     << ENUM_NAME(batch->transaction_type(), TransactionType);
          break;
      }

      break;
    }
    case internal::ForwardBatch::kBatchOrder: {
      auto& batch_order = forward_batch->batch_order();

      VLOG(1) << "Received order for batch " << batch_order.batch_id()
              << " from [" << from_machine_id << "]. Slot: " << batch_order.slot();

      all_logs_[from_replica].AddSlot(
          batch_order.slot(),
          batch_order.batch_id());
      break;
    }
    default:
      break;  
  }
}

void Scheduler::ProcessLocalQueueOrder(
    const internal::LocalQueueOrder& order) {
  VLOG(1) << "Received local queue order. Slot id: "
          << order.slot() << ". Queue id: " << order.queue_id(); 
  local_interleaver_.AddSlot(order.slot(), order.queue_id());
}

void Scheduler::ProcessRemoteReadResult(
    internal::Request&& req) {
  auto txn_id = req.remote_read_result().txn_id();
  auto& holder = all_txns_[txn_id];
  // A transaction might have a holder but not run yet if there are not
  // enough workers. In that case, a remote read is still considered an
  // early remote read.
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

  // Add stats for local logs
  stats.AddMember(
      StringRef(LOCAL_LOG_NUM_BUFFERED_SLOTS),
      local_interleaver_.NumBufferedSlots(),
      alloc);
  stats.AddMember(
      StringRef(LOCAL_LOG_NUM_BUFFERED_BATCHES_PER_QUEUE),
      ToJsonArrayOfKeyValue(local_interleaver_.NumBufferedBatchesPerQueue(), alloc),
      alloc);
  

  // Add stats for global logs
  stats.AddMember(
      StringRef(GLOBAL_LOG_NUM_BUFFERED_SLOTS_PER_REGION),
      ToJsonArrayOfKeyValue(
          all_logs_,
          [](const BatchLog& batch_log) { return batch_log.NumBufferedSlots(); },
          alloc),
      alloc);

  stats.AddMember(
      StringRef(GLOBAL_LOG_NUM_BUFFERED_BATCHES_PER_REGION),
      ToJsonArrayOfKeyValue(
          all_logs_,
          [](const BatchLog& batch_log) { return batch_log.NumBufferedBatches(); },
          alloc),
      alloc);

  // Add stats from the lock manager
  lock_manager_.GetStats(stats, level);

  // Write JSON object to a buffer and send back to the server
  rapidjson::StringBuffer buf;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
  stats.Accept(writer);

  internal::Response res;
  res.mutable_stats()->set_id(stats_request.id());
  res.mutable_stats()->set_stats_json(buf.GetString());
  SendSameMachine(res, SERVER_CHANNEL);
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

  // If a remaster transaction, trigger any unblocked txns
  if (txn->procedure_case() == Transaction::ProcedureCase::kNewMaster) {
    auto key = txn->write_set().begin()->first;
    auto counter = txn->internal().master_metadata().at(key).master() + 1;
    ProcessRemasterResult(remaster_manager_.RemasterOccured(key, counter));
  }

  SendToCoordinatingServer(txn_id);
  all_txns_.erase(txn_id);
}

void Scheduler::SendToCoordinatingServer(TxnId txn_id) {
  // Release txn so it can be passed to request
  auto& txn_holder = all_txns_[txn_id];
  Transaction txn_copy = *txn_holder.GetTransaction();

  // Send the txn back to the coordinating server
  Request req;
  auto completed_sub_txn = req.mutable_completed_subtxn();
  completed_sub_txn->set_allocated_txn(&txn_copy);
  completed_sub_txn->set_partition(config_->GetLocalPartition());
  for (auto p : txn_holder.InvolvedPartitions()) {
    completed_sub_txn->add_involved_partitions(p);
  }

  RecordTxnEvent(
      config_,
      txn_copy.mutable_internal(),
      TransactionEvent::EXIT_SCHEDULER);

  auto coordinating_server = MakeMachineIdAsString(
      txn_copy.internal().coordinating_server());
  Send(req, coordinating_server, SERVER_CHANNEL);
}

/***********************************************
                Logs Management
***********************************************/

void Scheduler::MaybeUpdateLocalLog() {
  auto local_partition = config_->GetLocalPartition();
  while (local_interleaver_.HasNextBatch()) {
    auto slot_id_and_batch_id = local_interleaver_.NextBatch();
    auto slot_id = slot_id_and_batch_id.first;
    auto batch_id = slot_id_and_batch_id.second;

    // Replicate to the corresponding partition in other regions
    Request request;
    auto forward_batch_order = request.mutable_forward_batch()->mutable_batch_order();
    forward_batch_order->set_batch_id(batch_id);
    forward_batch_order->set_slot(slot_id);
    auto num_replicas = config_->GetNumReplicas();
    for (uint32_t rep = 0; rep < num_replicas; rep++) {
      SendSameChannel(
          request,
          MakeMachineIdAsString(rep, local_partition),
          rep + 1 < num_replicas /* has_more */);
    }
  }
}

void Scheduler::MaybeProcessNextBatchesFromGlobalLog() {
  // Interleave batches from local logs of all regions and the log of MULTI-HOME txn
  for (auto& pair : all_logs_) {
    auto& local_log = pair.second;
    while (local_log.HasNextBatch()) {
      auto batch = local_log.NextBatch().second;
      auto transactions = batch->mutable_transactions();

      VLOG(1) << "Processing batch " << batch->id() << " from global log";

      // Calling ReleaseLast creates an incorrect order of transactions in a batch; thus,
      // buffering them in a stack to reverse the order.
      std::stack<Transaction*> buffer;
      while (!transactions->empty()) {
        buffer.push(transactions->ReleaseLast());
      }

      while (!buffer.empty()) {
        auto txn = buffer.top();
        buffer.pop();

        auto txn_internal = txn->mutable_internal();

        // Transfer recorded events from batch to each txn in the batch
        txn_internal->mutable_events()->MergeFrom(batch->events());
        txn_internal->mutable_event_times()->MergeFrom(batch->event_times());
        txn_internal->mutable_event_machines()->MergeFrom(batch->event_machines());

        if (!AcceptTransaction(txn)) {
          continue;
        }

        auto txn_id = txn_internal->id();
        auto txn_type = txn_internal->type();
        switch (txn_type) {
          case TransactionType::SINGLE_HOME: {
            VLOG(2) << "Accepted SINGLE-HOME transaction " << txn_id;
            auto txn_holder = &all_txns_[txn_id];

            // Check that remaster txns aren't remastering to the same master
            if (txn->procedure_case() == Transaction::ProcedureCase::kNewMaster) {
              auto past_master = txn->internal().master_metadata().begin()->second.master();
              if (txn->new_master() == past_master) {
                TriggerPreDispatchAbort(txn_id);
                break;
              }
            }

            if (aborting_txns_.count(txn_id)) {
              AddTransactionToAbort(txn_id);
            } else {
              SendToRemasterManager(txn_holder);
            }
            break;
          }
          case TransactionType::LOCK_ONLY: {
            auto txn_replica_id = TransactionHolder::GetTransactionIdReplicaIdPair(txn);
            VLOG(2) << "Accepted LOCK-ONLY transaction "
                << txn_replica_id.first <<", " << txn_replica_id.second;
            auto txn_holder = &lock_only_txns_[txn_replica_id];

            if (aborting_txns_.count(txn_id)) {
              AddLockOnlyTransactionToAbort(txn_replica_id);
            } else {
              SendToRemasterManager(txn_holder);
            }
            break;
          }
          case TransactionType::MULTI_HOME: {
            VLOG(2) << "Accepted MULTI-HOME transaction " << txn_id;
            auto txn_holder = &all_txns_[txn_id];

            if (aborting_txns_.count(txn_id)) {
              AddTransactionToAbort(txn_id);
            } else {
              SendToLockManager(txn_holder);
            }
            break;
          }
          default:
            LOG(ERROR) << "Unknown transaction type";
            break;
        }
      }
    }
  }
}

/***********************************************
              Transaction Processing
***********************************************/

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
    }
    case VerifyMasterResult::WAITING: {
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
  unordered_set<TxnId> aborting_txn_ids;
  for (auto unblocked_txn_holder : result.should_abort) {
    aborting_txn_ids.insert(unblocked_txn_holder->GetTransaction()->internal().id());
  }
  for (auto txn_id : aborting_txn_ids) {
    TriggerPreDispatchAbort(txn_id);
  }
}

void Scheduler::SendToLockManager(const TransactionHolder* txn_holder) {
  auto txn_id = txn_holder->GetTransaction()->internal().id();
  auto txn_type = txn_holder->GetTransaction()->internal().type();
  switch(txn_type) {
    case TransactionType::SINGLE_HOME: {
      if (lock_manager_.AcceptTransactionAndAcquireLocks(*txn_holder)) {
        DispatchTransaction(txn_id);
      }
      break;
    }
    case TransactionType::MULTI_HOME: {
      if (lock_manager_.AcceptTransaction(*txn_holder)) {
        RecordTxnEvent(
            config_,
            txn_holder->GetTransaction()->mutable_internal(),
            TransactionEvent::ACCEPTED);
        DispatchTransaction(txn_id); 
      }
      break;
    }
    case TransactionType::LOCK_ONLY: {
      if (lock_manager_.AcquireLocks(*txn_holder)) {
        CHECK(all_txns_.count(txn_id) > 0) 
            << "Txn " << txn_id << " is not found for dispatching";
        DispatchTransaction(txn_id);
      }
      break;
    }
    default:
      LOG(ERROR) << "Unknown transaction type";
      break;
  }
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
    AddTransactionToAbort(txn_id);
  } else {
    VLOG(3) << "Defering abort until txn arrives: " << txn_id;
  }
}

void Scheduler::AddTransactionToAbort(TxnId txn_id) {
  CHECK(aborting_txns_.count(txn_id) > 0) << "Abort not triggered: " << txn_id;
  auto& txn_holder = all_txns_[txn_id];
  auto txn = txn_holder.GetTransaction();
  auto txn_type = txn->internal().type();

  txn->set_status(TransactionStatus::ABORTED);
  SendToCoordinatingServer(txn_id);

  if (txn_holder.InvolvedPartitions().size() > 1) {
    SendAbortToPartitions(txn_id);
  }

  if (txn_type == TransactionType::MULTI_HOME) {
    CollectLockOnlyTransactionsForAbort(txn_id);
  }

  MaybeFinishAbort(txn_id);
}

void Scheduler::AddLockOnlyTransactionToAbort(TxnIdReplicaIdPair txn_replica_id) {
  auto& txn_holder = lock_only_txns_[txn_replica_id];
  auto txn_id = txn_replica_id.first;
  CHECK(aborting_txns_.count(txn_id) > 0)
      << "Abort not triggered: " << txn_id << ", " << txn_replica_id.second;

  lock_only_txns_.erase(txn_replica_id);
  mh_abort_waiting_on_[txn_id] -= 1;

  // Check if this was the last lock-only
  MaybeFinishAbort(txn_id);
}

void Scheduler::CollectLockOnlyTransactionsForAbort(TxnId txn_id) {
  auto& txn_holder = all_txns_[txn_id];
  auto& involved_replicas = txn_holder.InvolvedReplicas();
  mh_abort_waiting_on_[txn_id] += involved_replicas.size();

  // release LOs in remaster manager
  ProcessRemasterResult(
      remaster_manager_.ReleaseTransaction(txn_id, involved_replicas));

  // release LOs in lock manager
  for (auto unblocked_txn : lock_manager_.ReleaseLocks(txn_holder)) {
    DispatchTransaction(unblocked_txn);
  }

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
      auto machine_id = MakeMachineIdAsString(local_replica, p);
      SendSameChannel(request, machine_id);
    }
  }
}

void Scheduler::MaybeFinishAbort(TxnId txn_id) {
  auto& txn_holder = all_txns_[txn_id];
  auto txn = txn_holder.GetTransaction();

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
}

/***********************************************
              Transaction Dispatch
***********************************************/

void Scheduler::DispatchTransaction(TxnId txn_id) {

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
  msg.Set(MM_PROTO, req);
  msg.SendTo(worker_socket_);
}

} // namespace slog