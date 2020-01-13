#include "module/scheduler.h"

#include "common/proto_utils.h"
#include "proto/internal.pb.h"

using std::make_shared;

namespace slog {

using internal::Request;
using internal::Response;

const string Scheduler::WORKER_IN("inproc://worker_in");

Scheduler::Scheduler(
    shared_ptr<Configuration> config,
    zmq::context_t& context,
    Broker& broker,
    shared_ptr<Storage<Key, Record>> storage)
  : ChannelHolder(broker.AddChannel(SCHEDULER_CHANNEL)),
    config_(config),
    worker_socket_(context, ZMQ_PUSH),
    lock_manager_(config) {
  poll_items_.push_back(GetChannelPollItem());
  poll_items_.push_back({
    static_cast<void*>(worker_socket_),
    0, /* fd */
    ZMQ_POLLIN,
    0 /* revent */
  });

  for (size_t i = 0; i < config->GetNumWorkers(); i++) {
    workers_.push_back(MakeRunnerFor<Worker>(context, storage));
  }
}

void Scheduler::SetUp() {
  worker_socket_.bind(WORKER_IN);
  for (auto& worker : workers_) {
    worker->StartInNewThread();
  }
}

void Scheduler::Loop() {
  zmq::poll(poll_items_);

  if (HasMessageFromChannel()) {
    MMessage msg;
    ReceiveFromChannel(msg);
    if (msg.IsProto<Request>()) {
      Request req;
      msg.GetProto(req);
      HandleInternalRequest(std::move(req), msg.GetIdentity());
      TryProcessingNextBatchesFromGlobalLog();
    } 
  }

  if (HasMessageFromWorker()) {
    MMessage msg(worker_socket_);
    Response res;
    msg.GetProto(res);
    if (res.type_case() == Response::kProcessTxn) {
      auto txn_id = res.process_txn().txn_id();
      auto ready_txns = lock_manager_.ReleaseLocks(*all_txns_.at(txn_id));
      all_txns_.erase(txn_id);
      for (auto txn_id : ready_txns) {
        TryDispatchingTransaction(txn_id);
      }
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
  auto machine_id = MakeMachineIdProto(from_machine_id);
  switch (req.type_case()) {
    case Request::kForwardBatch: 
      ProcessForwardBatchRequest(
          req.mutable_forward_batch(), machine_id.partition());
      break;
    case Request::kOrder:
      ProcessOrderRequest(req.order());
      break;
    default:
      break;
  }
}

void Scheduler::ProcessForwardBatchRequest(
    internal::ForwardBatchRequest* forward_batch,
    uint32_t queue_id) {
  interleaver_.AddBatch(
      queue_id, BatchPtr{forward_batch->release_batch()});
}

void Scheduler::ProcessOrderRequest(
    const internal::OrderRequest& order) {
  interleaver_.AddAgreedSlot(order.slot(), order.value());
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
        TransactionPtr txn(transactions->ReleaseLast());
        auto txn_id = txn->internal().id();
        all_txns_[txn_id] = std::move(txn);
        pending_txn_.insert(txn_id);
        TryDispatchingTransaction(txn_id);
      }
    }
  }
}

void Scheduler::TryDispatchingTransaction(TxnId txn_id) {
  if (pending_txn_.count(txn_id) == 0) {
    LOG(ERROR) << "Transaction " << txn_id << " is not in pending state";
    return;
  }

  if (!lock_manager_.AcquireLocks(*all_txns_[txn_id])) {
    return;
  }

  Request req;
  auto process_txn = req.mutable_process_txn();
  process_txn->mutable_txn()->CopyFrom(*all_txns_[txn_id]);
  MMessage msg;
  msg.Set(MM_PROTO, req);
  msg.SendTo(worker_socket_);

  pending_txn_.erase(txn_id);
}

} // namespace slog