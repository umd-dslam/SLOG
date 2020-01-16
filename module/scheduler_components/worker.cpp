#include "module/scheduler_components/worker.h"

#include <thread>

#include "common/proto_utils.h"
#include "module/scheduler.h"

namespace slog {

using internal::Request;
using internal::Response;

Worker::Worker(
    Scheduler& scheduler,
    zmq::context_t& context,
    shared_ptr<Storage<Key, Record>> storage)
  : scheduler_(scheduler),
    scheduler_socket_(context, ZMQ_DEALER),
    storage_(storage),
    // TODO: change this dynamically based on selected experiment
    stored_procedures_(new KeyValueStoredProcedures()) {}

void Worker::SetUp() {
  scheduler_socket_.connect(Scheduler::WORKERS_ENDPOINT);
  // Send an empty response to tell Scheduler that this worker is ready
  SendToScheduler(Response());
}

void Worker::Loop() {
  MMessage msg(scheduler_socket_);
  Request req;
  if (!msg.GetProto(req)) {
    return;
  }
  switch (req.type_case()) {
    case Request::kProcessTxn: {
      CHECK(txn_state_ == nullptr)
          << "Another transaction is being processed by this worker";
      auto txn_id = req.process_txn().txn_id();
      txn_state_.reset(new TransactionState(txn_id));
      if (PrepareTransaction()) {
        // Immediately execute and commit this transaction if this
        // machine is a passive participant
        ExecuteAndCommitTransaction();
      }
      break;
    }
    case Request::kRemoteReadResult: {
      auto& remote_read_result = req.remote_read_result();
      if (ApplyRemoteReadResult(remote_read_result)) {
        // Execute and commit this transaction when all remote reads
        // are received
        ExecuteAndCommitTransaction();
      }
      break;
    }
    default:
      break;
  }
}

bool Worker::PrepareTransaction() {
  CHECK(txn_state_ != nullptr) << "There is no in-progress transactions";

  bool is_passive_participant = true;

  auto txn_id  = txn_state_->txn_id;
  auto config = scheduler_.config_;
  auto& txn = scheduler_.all_txns_.at(txn_id).txn;

  Request request;
  auto remote_read_result = request.mutable_remote_read_result();
  auto remote_reads = remote_read_result->mutable_reads();
  remote_read_result->set_txn_id(txn_id);
  remote_read_result->set_partition(config->GetLocalPartition());

  for (auto& key_value : *txn->mutable_read_set()) {
    const auto& key = key_value.first;
    auto partition = config->KeyToPartition(key);
    if (partition == config->GetLocalPartition()) {
      Record record;
      storage_->Read(key, record);
      key_value.second = record.value;
      (*remote_reads)[key] = record.value;
    } else {
      txn_state_->awaited_passive_participants.insert(partition);
    }
  }

  for (auto& key_value : *txn->mutable_write_set()) {
    const auto& key = key_value.first;
    auto partition = config->KeyToPartition(key);
    if (partition == config->GetLocalPartition()) {

      is_passive_participant = false;

      Record record;
      storage_->Read(key, record);
      key_value.second = record.value;
      (*remote_reads)[key] = record.value;
    } else {
      txn_state_->active_participants.insert(partition);
    }
  }

  // Send reads to all active participants in local replica
  auto local_replica = config->GetLocalReplica();
  for (auto participant : txn_state_->active_participants) {
    SendToScheduler(
        request, MakeMachineId(local_replica, participant));
  }

  return is_passive_participant;
}

bool Worker::ApplyRemoteReadResult(
    const internal::RemoteReadResult& read_result) {
  CHECK(txn_state_ != nullptr) << "There is no in-progress transactions";

  auto txn_id = txn_state_->txn_id;
  CHECK_EQ(txn_id, read_result.txn_id())
      << "Remote read result belongs to a different transaction. "
      << "Expected: " << txn_id
      << ". Received: " << read_result.txn_id();
  
  auto& awaited = txn_state_->awaited_passive_participants;
  if (awaited.count(read_result.partition()) == 0) {
    return awaited.empty();
  }
  awaited.erase(read_result.partition());

  // Apply remote reads to local txn
  auto& txn = scheduler_.all_txns_.at(txn_id).txn;
  for (const auto& key_value : read_result.reads()) {
    const auto& key = key_value.first;
    const auto& value = key_value.second;
    (*txn->mutable_read_set())[key] = value;
  }

  return awaited.empty();
}

void Worker::ExecuteAndCommitTransaction() {
  CHECK(txn_state_ != nullptr) << "There is no in-progress transactions";

  auto txn_id = txn_state_->txn_id;
  auto& txn = scheduler_.all_txns_.at(txn_id).txn;

  // Execute the transaction code
  stored_procedures_->Execute(*txn);

  // Apply all writes to local storage
  auto& master_metadata = txn->internal().master_metadata();
  for (const auto& key_value : txn->write_set()) {
    const auto& key = key_value.first;
    const auto& value = key_value.second;
    Record record;
    bool found = storage_->Read(key_value.first, record);
    if (!found) {
      CHECK(master_metadata.contains(key))
          << "Master metadata for key \"" << key << "\" is missing";
      record.metadata = master_metadata.at(key);
    }
    record.value = value;
    storage_->Write(key, record);
  }
  for (const auto& key : txn->delete_set()) {
    storage_->Delete(key);
  }

  // Response back to the scheduler
  Response res;
  res.mutable_process_txn()->set_txn_id(txn_id);
  SendToScheduler(res);

  txn_state_.reset();
}

void Worker::SendToScheduler(
    const google::protobuf::Message& req_or_res,
    string&& forward_to_machine) {
  MMessage msg;
  msg.Set(MM_PROTO, req_or_res);
  msg.Set(MM_PROTO + 1, std::move(forward_to_machine));
  msg.SendTo(scheduler_socket_);
}

} // namespace slog