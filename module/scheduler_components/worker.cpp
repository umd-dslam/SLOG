#include "module/scheduler_components/worker.h"

#include <thread>

#include "common/proto_utils.h"
#include "module/scheduler.h"

namespace slog {

using internal::Request;
using internal::Response;

Worker::Worker(
    ConfigurationPtr config,
    zmq::context_t& context,
    shared_ptr<Storage<Key, Record>> storage)
  : config_(config),
    scheduler_socket_(context, ZMQ_DEALER),
    storage_(storage),
    // TODO: change this dynamically based on selected experiment
    commands_(new KeyValueCommands()) {
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
  SendReadyResponse();
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
    case Request::kWorker: {
      ProcessWorkerRequest(req.worker());
      break;
    }
    case Request::kRemoteReadResult: {
      ProcessRemoteReadResult(req.remote_read_result());
      break;
    }
    default:
      break;
  }
}

void Worker::ProcessWorkerRequest(const internal::WorkerRequest& worker_request) {
  auto txn = reinterpret_cast<Transaction*>(worker_request.txn_ptr());
  auto txn_id = txn->internal().id();

  const auto& state = InitializeTransactionState(txn);

  PopulateDataFromLocalStorage(txn);

  // Send local reads to all remote active partitions
  if (state.has_local_reads) {
    Request request;
    auto rrr = request.mutable_remote_read_result();
    rrr->set_txn_id(txn_id);
    rrr->set_partition(config_->GetLocalPartition());
    auto reads_to_be_sent = rrr->mutable_reads();
    auto local_replica = config_->GetLocalReplica();
    for (auto& key_value : txn->read_set()) {
      (*reads_to_be_sent)[key_value.first] = key_value.second;
    }
    for (auto p : state.remote_active_partitions) {
      auto machine_id = MakeMachineIdAsString(local_replica, p);
      SendToScheduler(request, std::move(machine_id));
    }
  }

  // If the txn does not do any write or does not have to wait for remote read, 
  // move on with executing this transaction. Otherwise, notify the scheduler
  // that this worker is free
  if (!state.has_local_writes || state.remote_passive_partitions.empty()) {
    VLOG(3) << "Execute txn " << txn_id << " without remote reads";
    ExecuteAndCommitTransaction(txn_id);
  } else {
    VLOG(3) << "Defer executing txn " << txn->internal().id() << " until having enough remote reads";
    SendReadyResponse();
  }
}

TransactionState& Worker::InitializeTransactionState(Transaction* txn) {
  auto txn_id = txn->internal().id();
  auto res = txn_states_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(txn_id),
      std::forward_as_tuple(txn));

  CHECK(res.second) << "Transaction " << txn_id << " has already been dispatched to this worker";

  auto& state = txn_states_[txn_id];
  auto local_partition = config_->GetLocalPartition();
  vector<Key> key_vec;
    
  // Collect all remote passive partitions
  state.has_local_reads = false;
  key_vec.reserve(txn->read_set_size());
  for (auto& key_value : *txn->mutable_read_set()) {
    const auto& key = key_value.first;
    auto partition = config_->GetPartitionOfKey(key);
    state.partitions.insert(partition);
    if (partition == local_partition) {
      state.has_local_reads = true;
    } else {
      state.remote_passive_partitions.insert(partition);
      key_vec.push_back(key);
    }
  }
  // Remove all keys that are to be read by a remote partition
  for (const auto& key : key_vec) {
    txn->mutable_read_set()->erase(key);
  }

  // Collect all remote active partitions
  state.has_local_writes = false;
  key_vec.reserve(txn->write_set_size());
  for (auto& key_value : *txn->mutable_write_set()) {
    const auto& key = key_value.first;
    auto partition = config_->GetPartitionOfKey(key);
    state.partitions.insert(partition);
    if (partition == local_partition) {
      state.has_local_writes = true;
    } else {
      state.remote_active_partitions.insert(partition);
      key_vec.push_back(key);
    }
  }
  // Remove all keys that are to be written by a remote partition
  for (const auto& key : key_vec) {
    txn->mutable_write_set()->erase(key);
  }

  return state;
}

void Worker::PopulateDataFromLocalStorage(Transaction* txn) {
  for (auto& key_value : *txn->mutable_read_set()) {
    Record record;
    storage_->Read(key_value.first, record);
    key_value.second = record.value;
  }

  for (auto& key_value : *txn->mutable_write_set()) {
    Record record;
    storage_->Read(key_value.first, record);
    key_value.second = record.value;
  }
}

void Worker::ProcessRemoteReadResult(const internal::RemoteReadResult& read_result) {
  auto txn_id = read_result.txn_id();

  CHECK(txn_states_.count(txn_id) > 0)
      << "Transaction " << txn_id << " does not exist for remote read result";

  auto txn = txn_states_[txn_id].txn;
  auto& rpp = txn_states_[txn_id].remote_passive_partitions;

  if (rpp.count(read_result.partition()) > 0) {
    rpp.erase(read_result.partition());
    // Apply remote reads to local txn
    for (const auto& key_value : read_result.reads()) {
      (*txn->mutable_read_set())[key_value.first] = key_value.second;
    }
  }

  // If all remote reads arrived, move on with executing this transaction.
  // Otherwise, notify the scheduler that this worker is free
  if (rpp.empty()) {
    VLOG(3) << "Execute txn " << txn_id << " after receving all remote read result";
    ExecuteAndCommitTransaction(txn_id);
  } else {
    SendReadyResponse();
  }
}

void Worker::ExecuteAndCommitTransaction(TxnId txn_id) {
  const auto& state = txn_states_[txn_id];
  auto txn = state.txn;

  // Execute the transaction code
  commands_->Execute(*txn);

  // Apply all writes to local storage if the transaction is not aborted
  if (txn->status() == TransactionStatus::COMMITTED) {
    auto& master_metadata = txn->internal().master_metadata();
    for (const auto& key_value : txn->write_set()) {
      const auto& key = key_value.first;
      if (config_->KeyIsInLocalPartition(key)) {
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
      if (config_->KeyIsInLocalPartition(key)) {
        storage_->Delete(key);
      }
    }
  }

  // Response back to the scheduler
  Response res;
  res.mutable_worker()->set_load(txn_states_.size());
  res.mutable_worker()->set_has_txn_id(true);
  res.mutable_worker()->set_txn_id(txn_id);
  for (auto p : state.partitions) {
    res.mutable_worker()
        ->mutable_participants()
        ->Add(p);
  }
  SendToScheduler(res);

  // Done with this txn. Remove it from the state map
  txn_states_.erase(txn_id);
}

void Worker::SendReadyResponse() {
  Response res;
  res.mutable_worker()->set_load(txn_states_.size());
  res.mutable_worker()->set_has_txn_id(false);
  SendToScheduler(res);
}

void Worker::SendToScheduler(
    const google::protobuf::Message& req_or_res,
    string&& forward_to_machine) {
  MMessage msg;
  msg.Set(MM_PROTO, req_or_res);
  // MM_PROTO + 1 is a convention between the Scheduler and
  // Worker to specify the destination of this message
  msg.Set(MM_PROTO + 1, std::move(forward_to_machine));
  msg.SendTo(scheduler_socket_);
}

} // namespace slog