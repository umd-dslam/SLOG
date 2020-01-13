#include "module/scheduler_components/worker.h"

#include <thread>

#include "module/scheduler.h"
#include "module/scheduler_components/outputter.h"

namespace slog {

using internal::Request;
using internal::Response;

Worker::Worker(
    zmq::context_t& context,
    shared_ptr<Storage<Key, Record>> storage)
  : scheduler_socket_(context, ZMQ_REP),
    outputter_socket_(context, ZMQ_PUSH),
    storage_(storage) {}

void Worker::SetUp() {
  scheduler_socket_.connect(Scheduler::WORKER_IN);
  outputter_socket_.connect(Outputter::WORKER_OUT);
}

void Worker::Loop() {
  MMessage msg(scheduler_socket_);
  if (!msg.IsProto<Request>()) {
    return;
  }
  Request req;
  msg.GetProto(req);
  if (req.type_case() != Request::kProcessTxn) {
    return;
  }

  auto txn = req.mutable_process_txn()->release_txn();
  ProcessTransaction(txn);

  ResponseToScheduler(txn->internal().id());
  ForwardToOutputter(txn);
}

void Worker::ProcessTransaction(Transaction* txn) {
  for (auto& key_value : *txn->mutable_read_set()) {
    Record record;
    storage_->Read(key_value.first, record);
    key_value.second = record.value;
  }

  for (auto& key_value : txn->write_set()) {
    Record record;
    bool found = storage_->Read(key_value.first, record);
    if (!found) {
      // TODO: need a way to deterministically assign master info
      record.metadata = {};
    }
    record.value = key_value.second;
    storage_->Write(key_value.first, record);
  }

  // TODO: change to something more sensible
  std::this_thread::sleep_for(10ms);
}

void Worker::ResponseToScheduler(TxnId txn_id) {
  Response process_txn_res;
  process_txn_res.mutable_process_txn()->set_txn_id(txn_id);
  MMessage msg;
  msg.Set(MM_PROTO, process_txn_res);
  msg.SendTo(scheduler_socket_);
}

void Worker::ForwardToOutputter(Transaction* txn) {
  Request forward_req;
  forward_req.mutable_forward_txn()->set_allocated_txn(txn);
  MMessage msg;
  msg.Set(MM_PROTO, forward_req);
  msg.SendTo(outputter_socket_);
}

} // namespace slog