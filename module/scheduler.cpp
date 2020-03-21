#include "module/scheduler.h"

#include "common/proto_utils.h"
#include "proto/internal.pb.h"

using std::make_shared;

namespace slog {

using internal::Request;
using internal::Response;

const string Scheduler::WORKERS_ENDPOINT("inproc://workers");

Scheduler::Scheduler(
    ConfigurationPtr config,
    zmq::context_t& context,
    Broker& broker,
    shared_ptr<Storage<Key, Record>> storage)
  : ChannelHolder(broker.AddChannel(SCHEDULER_CHANNEL)),
    // All single-home txn logs take indices in the [0, num_replicas - 1]
    // range, num_replicas can be used as the marker for multi-home txn log
    kMultiHomeTxnLogMarker(config->GetNumReplicas()),
    config_(config),
    worker_socket_(context, ZMQ_ROUTER),
    lock_manager_(config) {
  worker_socket_.setsockopt(ZMQ_LINGER, 0);
  poll_items_.push_back(GetChannelPollItem());
  poll_items_.push_back({
      static_cast<void*>(worker_socket_),
      0, /* fd */
      ZMQ_POLLIN,
      0 /* revent */});

  for (size_t i = 0; i < config->GetNumWorkers(); i++) {
    workers_.push_back(MakeRunnerFor<Worker>(config, context, storage));
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
}

void Scheduler::Loop() {
  zmq::poll(poll_items_, MODULE_POLL_TIMEOUT_MS);

  if (HasMessageFromChannel()) {
    MMessage msg;
    ReceiveFromChannel(msg);
    Request req;
    if (msg.GetProto(req)) {
      HandleInternalRequest(std::move(req), msg.GetIdentity());
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
      // A worker finishes processing a transaction. Mark this 
      // worker as ready for another transaction.
      Response res;
      msg.GetProto(res);
      ready_workers_.push(msg.GetIdentity());
      // Handle the results produced by the worker
      HandleResponseFromWorker(std::move(res));
      // Since we now have a free worker, try dispatching a ready
      // txn if one is in queue.
      MaybeDispatchNextTransaction();
    }
  }
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
      ProcessRemoteReadResult(std::move(req));
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
 
      switch (batch->transaction_type()) {
        case TransactionType::SINGLE_HOME: {
          VLOG(1) << "Received data for single-home batch " << batch->id()
              << " from [" << from_machine_id
              << "]. Number of txns: " << batch->transactions_size();
          // If this batch comes from the local region, put it into the local interleaver
          if (from_replica == config_->GetLocalReplica()) {
            local_interleaver_.AddBatchId(
                machine_id.partition(),
                forward_batch->same_origin_position(),
                batch->id());
          }
          
          all_logs_[from_replica].AddBatch(std::move(batch));
          break;
        }
        case TransactionType::MULTI_HOME: {
          VLOG(1) << "Received data for multi-home batch " << batch->id()
              << " from [" << from_machine_id
              << "]. Number of txns: " << batch->transactions_size();
          // Multi-home txns are already ordered with respect to each other
          // and their IDs have been replaced with slot id in the orderer module
          // so here their id and slot id are the same
          all_logs_[kMultiHomeTxnLogMarker].AddSlot(batch->id(), batch->id());
          all_logs_[kMultiHomeTxnLogMarker].AddBatch(std::move(batch));
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
  // A transaction might have a holder but not run yet if there is not
  // enough worker. In that case, a remote read is still considered an
  // early remote read.
  if (holder.txn != nullptr && !holder.worker.empty()) {
    VLOG(2) << "Got remote read result for txn " << txn_id;
    SendToWorker(std::move(req), holder.worker);
  } else {
    // Save the remote reads that come before the txn
    // is processed by this partition
    //
    // TODO: If this request is not needed but still arrives AFTER the transaction 
    // is already commited, it will be stuck in early_remote_reads forever.
    // Consider garbage collecting them if needed
    VLOG(2) << "Got early remote read result for txn " << txn_id;
    holder.early_remote_reads.push_back(std::move(req));
  }
}

/***********************************************
              Worker Responses
***********************************************/

void Scheduler::HandleResponseFromWorker(Response&& res) {
  if (res.type_case() != Response::kProcessTxn) {
    return;
  }
  // This txn is done so remove it from the txn list
  auto txn_id = res.process_txn().txn_id();
  auto txn = all_txns_[txn_id].txn;
  all_txns_.erase(txn_id);

  // Release locks held by this txn. Enqueue the txns that
  // become ready thanks to this release.
  auto unlocked_txns = lock_manager_.ReleaseLocks(*txn);
  for (auto unlocked_txn : unlocked_txns) {
    EnqueueTransaction(unlocked_txn);
  }

  // Send the txn back to the coordinating server if need to
  auto local_partition = config_->GetLocalPartition();
  auto& participants = res.process_txn().participants();
  if (std::find(
      participants.begin(),
      participants.end(),
      local_partition) != participants.end()) {
    auto coordinating_server = MakeMachineIdAsString(
          txn->internal().coordinating_server());
    Request req;
    auto completed_sub_txn = req.mutable_completed_subtxn();
    completed_sub_txn->set_allocated_txn(txn);
    completed_sub_txn->set_partition(config_->GetLocalPartition());
    for (auto p : participants) {
      completed_sub_txn->add_involved_partitions(p);
    }
    Send(req, coordinating_server, SERVER_CHANNEL);
  }
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
    for (uint32_t rep = 0; rep < config_->GetNumReplicas(); rep++) {
      SendSameChannel(request, MakeMachineIdAsString(rep, local_partition));
    }
  }
}

void Scheduler::MaybeProcessNextBatchesFromGlobalLog() {
  // Interleave batches from local logs of all regions and the log of multi-home txn
  for (auto& pair : all_logs_) {
    auto& local_log = pair.second;
    while (local_log.HasNextBatch()) {
      auto batch = local_log.NextBatch().second;
      auto transactions = batch->mutable_transactions();

      while (!transactions->empty()) {
        auto txn = transactions->ReleaseLast();
        auto txn_type = txn->internal().type();

        switch (txn_type) {
          case TransactionType::SINGLE_HOME: {
            auto txn_id = txn->internal().id();
            // TODO: remove txn holder if the txn does not touch anything
            // in the current partition
            auto& holder = all_txns_[txn_id];
            holder.txn = txn;
            if (lock_manager_.RegisterTxnAndAcquireLocks(*holder.txn)) {
              EnqueueTransaction(txn_id); 
            }
            break;
          }
          case TransactionType::MULTI_HOME: {
            auto txn_id = txn->internal().id();
            // TODO: remove txn holder if the txn does not touch anything
            // in the current partition
            auto& holder = all_txns_[txn_id];
            holder.txn = txn;
            if (lock_manager_.RegisterTxn(*holder.txn)) {
              EnqueueTransaction(txn_id); 
            }
            break;
          }
          case TransactionType::LOCK_ONLY: {
            if (lock_manager_.AcquireLocks(*txn)) {
              auto txn_id = txn->internal().id();
              CHECK(all_txns_.count(txn_id) > 0) 
                  << "Txn " << txn_id << " is not found for dispatching";
              EnqueueTransaction(txn_id);
            }
            delete txn;
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
              Transaction Dispatch
***********************************************/

void Scheduler::EnqueueTransaction(TxnId txn_id) {
  VLOG(2) << "Enqueued txn " << txn_id;
  ready_txns_.push(txn_id);
  MaybeDispatchNextTransaction();
}

void Scheduler::MaybeDispatchNextTransaction() {
  if (ready_workers_.empty() || ready_txns_.empty()) {
    return;
  }
  // Pop next ready txn in queue
  auto txn_id = ready_txns_.front();
  ready_txns_.pop();
  VLOG(2) << "Dispatched txn " << txn_id;

  auto& holder = all_txns_[txn_id];

  // Pop next ready worker in queue
  holder.worker = ready_workers_.front();
  ready_workers_.pop();

  // Prepare a request with the txn to be sent to the worker
  Request req;
  auto process_txn = req.mutable_process_txn();
  process_txn->set_txn_ptr(reinterpret_cast<uint64_t>(holder.txn));

  // The transaction need always be sent to a worker before
  // any remote reads is sent for that transaction
  // TODO: pretty sure that the order of messages follow the order of these
  // function calls but should look a bit more into ZMQ ordering to be sure
  SendToWorker(std::move(req), holder.worker);
  for (auto& remote_read : holder.early_remote_reads) {
    SendToWorker(std::move(remote_read), holder.worker);
  }
}

void Scheduler::SendToWorker(internal::Request&& req, const string& worker) {
  MMessage msg;
  msg.SetIdentity(worker);
  msg.Set(MM_PROTO, req);
  msg.SendTo(worker_socket_);
}

} // namespace slog