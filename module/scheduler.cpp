#include "module/scheduler.h"

#include "common/proto_utils.h"
#include "proto/internal.pb.h"

using std::make_shared;

namespace slog {

using internal::Request;
using internal::Response;

const string Scheduler::WORKERS_ENDPOINT("inproc://workers");

Scheduler::Scheduler(
    shared_ptr<Configuration> config,
    zmq::context_t& context,
    Broker& broker,
    shared_ptr<Storage<Key, Record>> storage)
  : ChannelHolder(broker.AddChannel(SCHEDULER_CHANNEL)),
    config_(config),
    worker_socket_(context, ZMQ_ROUTER),
    lock_manager_(config) {
  worker_socket_.setsockopt(ZMQ_LINGER, 0);
  poll_items_.push_back(GetChannelPollItem());
  poll_items_.push_back({
    static_cast<void*>(worker_socket_),
    0, /* fd */
    ZMQ_POLLIN,
    0 /* revent */
  });

  for (size_t i = 0; i < config->GetNumWorkers(); i++) {
    workers_.push_back(MakeRunnerFor<Worker>(*this, context, storage));
  }
}

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
    MMessage msg(worker_socket_);
    if (msg.IsProto<Request>()) {
      // Forward requests from worker to remote machines
      Request forwarded_req;
      msg.GetProto(forwarded_req);
      string destination;
      msg.GetString(destination, MM_PROTO + 1);

      SendSameChannel(forwarded_req, destination);
    } else if (msg.IsProto<Response>()) {
      Response res;
      msg.GetProto(res);
      ready_workers_.push(msg.GetIdentity());

      HandleResponseFromWorker(std::move(res));
    }
  }
}

bool Scheduler::HasMessageFromChannel() const {
  return poll_items_[0].revents & ZMQ_POLLIN;
}

bool Scheduler::HasMessageFromWorker() const {
  return poll_items_[1].revents & ZMQ_POLLIN;
}

void Scheduler::HandleInternalRequest(
    Request&& req,
    const string& from_machine_id) {
  switch (req.type_case()) {
    case Request::kForwardBatch: 
      ProcessForwardBatchRequest(
          req.mutable_forward_batch(), from_machine_id);
      break;
    case Request::kPaxosOrder:
      ProcessBatchOrder(req.paxos_order());
      break;
    case Request::kRemoteReadResult:
      ProcessRemoteReadResult(std::move(req));
      break;
    default:
      break;
  }
  TryProcessingNextBatchesFromGlobalLog();
}

void Scheduler::ProcessForwardBatchRequest(
    internal::ForwardBatchRequest* forward_batch,
    const string& from_machine_id) {
  auto batch_id = forward_batch->batch().id();
  auto machine_id = from_machine_id.empty() 
    ? config_->GetLocalMachineIdAsProto() 
    : MakeMachineIdProto(from_machine_id);

  VLOG(1) << "Received a batch with id " << batch_id << ": " 
          << forward_batch->batch().transactions_size() << " transaction(s)";

  interleaver_.AddBatch(
      machine_id.partition(), BatchPtr{forward_batch->release_batch()});

  // Only acknowledge if this batch is from the same replica
  if (machine_id.replica() == config_->GetLocalReplica()) {
    Response res;
    res.mutable_forward_batch()->set_batch_id(batch_id);
    Send(res, from_machine_id, SEQUENCER_CHANNEL);
  }
}

void Scheduler::ProcessBatchOrder(
    const internal::PaxosOrder& order) {
  VLOG(1) << "Received batch order. Slot id: "
          << order.slot() << ". Queue id: " << order.value(); 
  interleaver_.AddAgreedSlot(order.slot(), order.value());
}

void Scheduler::ProcessRemoteReadResult(
    internal::Request&& req) {
  auto txn_id = req.remote_read_result().txn_id();
  auto& holder = all_txns_[txn_id];
  if (holder.txn != nullptr) {
    VLOG(2) << " Got remote read result";
    SendToWorker(std::move(req), holder.worker);
  } else {
    // Save the remote reads that come before the txn
    // is processed by this partition
    //
    // TODO: If this request is not needed but still arrives and arrives AFTER
    // the transaction is already commited, it will be stuck in early_remote_reads
    // forever. Consider garbage collect this if it waits for too long.
    VLOG(2) << "Got early remote read result";
    holder.early_remote_reads.push_back(std::move(req));
  }
}

void Scheduler::TryProcessingNextBatchesFromGlobalLog() {
  // Update the local log of the local replica
  auto local_replica = config_->GetLocalReplica();
  while (interleaver_.HasNextBatch()) {
    auto slot_id_and_batch = interleaver_.NextBatch();
    local_logs_[local_replica].AddBatch(
        slot_id_and_batch.first,
        slot_id_and_batch.second);
  }

  // Interleave batches from all local logs
  for (auto& pair : local_logs_) {
    auto& local_log = pair.second;
    while (local_log.HasNextBatch()) {
      auto batch = local_log.NextBatch();
      auto transactions = batch->mutable_transactions();

      while (!transactions->empty()) {
        auto txn = transactions->ReleaseLast();
        auto txn_id = txn->internal().id();
        auto& holder = all_txns_[txn_id];
        holder.txn.reset(txn);
        if (lock_manager_.AcquireLocks(*holder.txn)) {
          DispatchTransaction(txn_id); 
        }
      }
    }
  }
}

void Scheduler::HandleResponseFromWorker(Response&& res) {
  if (res.type_case() != Response::kProcessTxn) {
    return;
  }
  // This txn is done so remove it from the txn list
  auto txn_id = res.process_txn().txn_id();
  auto txn = all_txns_.at(txn_id).txn.release();
  all_txns_.erase(txn_id);

  // Release locks held by this txn. Dispatch the txns that
  // become ready thanks to this release.
  auto ready_txns = lock_manager_.ReleaseLocks(*txn);
  for (auto ready_txn_id : ready_txns) {
    DispatchTransaction(ready_txn_id);
  }

  // Send the txn back to the coordinating server if need to
  auto local_partition = config_->GetLocalPartition();
  auto& participants = res.process_txn().participants();
  if (std::find(
      participants.begin(),
      participants.end(),
      local_partition) != participants.end()) {
    auto coordinating_server = MakeMachineId(
          txn->internal().coordinating_server());
    Request req;
    auto forward_sub_txn = req.mutable_forward_sub_txn();
    forward_sub_txn->set_allocated_txn(txn);
    forward_sub_txn->set_partition(config_->GetLocalPartition());
    for (auto p : participants) {
      forward_sub_txn->add_involved_partitions(p);
    }
    Send(req, coordinating_server, SERVER_CHANNEL);
  }
}

void Scheduler::DispatchTransaction(TxnId txn_id) {
  VLOG(1) << "Dispatched txn " << txn_id;

  auto& holder = all_txns_.at(txn_id);

  holder.worker = ready_workers_.front();
  ready_workers_.pop();

  Request req;
  auto process_txn = req.mutable_process_txn();
  process_txn->set_txn_id(txn_id);

  // TODO: pretty sure that the order of messages follow the order of these
  // function calls but investigate a bit about ZMQ ordering to be sure
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