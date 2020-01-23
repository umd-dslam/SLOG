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
    stored_procedures_(new KeyValueStoredProcedures()) {
  scheduler_socket_.setsockopt(ZMQ_LINGER, 0);
  poll_item_ = {
    static_cast<void*>(scheduler_socket_),
    0, /* fd */
    ZMQ_POLLIN,
    0 /* revent */
  };
}

void Worker::SetUp() {
  scheduler_socket_.connect(Scheduler::WORKERS_ENDPOINT);
  // Send an empty response to tell Scheduler that this worker is ready
  SendToScheduler(Response());
}

void Worker::Loop() {
  if (!zmq::poll(&poll_item_, 1, MODULE_POLL_TIMEOUT_MS)) {
    return;
  }

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
        // machine is a passive participant or if it is an active
        // one but does not have to wait for other partitions
        ExecuteAndCommitTransaction();
      }
      break;
    }
    case Request::kRemoteReadResult: {
      if (txn_state_ == nullptr) {
        LOG(ERROR) << "There is no in-progress transaction to apply remote reads to";
        break;
      }
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
  auto local_partition = config->GetLocalPartition();
  auto& txn = scheduler_.all_txns_.at(txn_id).txn;

  Request request;
  auto remote_read_result = request.mutable_remote_read_result();
  auto to_be_sent = remote_read_result->mutable_reads();
  remote_read_result->set_txn_id(txn_id);
  remote_read_result->set_partition(local_partition);

  vector<Key> remote_read_keys;
  for (auto& key_value : *txn->mutable_read_set()) {
    const auto& key = key_value.first;
    auto partition = config->KeyToPartition(key);
    if (partition == local_partition) {
      Record record;
      storage_->Read(key, record);
      key_value.second = record.value;
      (*to_be_sent)[key] = record.value;
    } else {
      remote_read_keys.push_back(key);
      txn_state_->awaited_passive_participants.insert(partition);
    }
    txn_state_->participants.insert(partition);
  }
  // Remove all keys that are to be read in a remote partition
  for (const auto& key : remote_read_keys) {
    txn->mutable_read_set()->erase(key);
  }

  vector<Key> remote_write_keys;
  for (auto& key_value : *txn->mutable_write_set()) {
    const auto& key = key_value.first;
    auto partition = config->KeyToPartition(key);
    if (partition == local_partition) {

      is_passive_participant = false;

      Record record;
      storage_->Read(key, record);
      key_value.second = record.value;
    } else {
      remote_write_keys.push_back(key);
      txn_state_->active_participants.insert(partition);
    }
    txn_state_->participants.insert(partition);
  }
  // Remove all keys that are to be written in a remote partition
  for (const auto& key : remote_write_keys) {
    txn->mutable_write_set()->erase(key);
  }

  // Send reads to all active participants in local replica if
  // the current partition is one of the participants
  if (!to_be_sent->empty() && txn_state_->participants.count(local_partition) > 0) {
    auto local_replica = config->GetLocalReplica();
    for (auto participant : txn_state_->active_participants) {
      auto machine_id = MakeMachineId(local_replica, participant);
      SendToScheduler(request, std::move(machine_id));
    }
  }

  return is_passive_participant || txn_state_->awaited_passive_participants.empty();
}

bool Worker::ApplyRemoteReadResult(
    const internal::RemoteReadResult& read_result) {
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
  auto config = scheduler_.config_;

  // Execute the transaction code
  stored_procedures_->Execute(*txn);

  // Apply all writes to local storage if the transaction is not aborted
  if (txn->status() == TransactionStatus::COMMITTED) {
    auto& master_metadata = txn->internal().master_metadata();
    for (const auto& key_value : txn->write_set()) {
      const auto& key = key_value.first;
      if (config->KeyIsInLocalPartition(key)) {
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
    }
    for (const auto& key : txn->delete_set()) {
      if (config->KeyIsInLocalPartition(key)) {
        storage_->Delete(key);
      }
    }
  }

  // Response back to the scheduler
  Response res;
  res.mutable_process_txn()->set_txn_id(txn_id);
  for (auto participant : txn_state_->participants) {
    res.mutable_process_txn()->mutable_participants()->Add(participant);
  }
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