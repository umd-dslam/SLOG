#include "module/scheduler_components/worker.h"
#include "module/scheduler_components/remaster_manager.h"

#include <thread>

#include "common/proto_utils.h"
#include "module/scheduler.h"

namespace slog {

using internal::Request;
using internal::Response;

Worker::Worker(
    const string& identity,
    ConfigurationPtr config,
    zmq::context_t& context,
    shared_ptr<Storage<Key, Record>> storage)
  : identity_(identity),
    config_(config),
    scheduler_socket_(context, ZMQ_DEALER),
    storage_(storage),
    // TODO: change this dynamically based on selected experiment
    commands_(new KeyValueCommands()) {
  scheduler_socket_.setsockopt(ZMQ_IDENTITY, identity);
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
  // Send an empty message to the scheduler to indicate
  // this worker is live
  SendToScheduler(Response());
}

void Worker::Loop() {
  if (zmq::poll(&poll_item_, 1, MODULE_POLL_TIMEOUT_MS)) {
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
  VLOG_EVERY_N(4, 5000/MODULE_POLL_TIMEOUT_MS) << "Worker " << identity_ << " is alive.";
}

void Worker::ProcessWorkerRequest(const internal::WorkerRequest& worker_request) {
  auto txn_holder = reinterpret_cast<TransactionHolder*>(worker_request.txn_holder_ptr());
  auto txn = txn_holder->GetTransaction();
  auto txn_internal = txn->mutable_internal();
  RecordTxnEvent(
      config_,
      txn_internal,
      TransactionEvent::ENTER_WORKER);

  auto txn_id = txn_internal->id();

  const auto& state = InitializeTransactionState(txn_holder);

  auto will_abort = false;
  switch(RemasterManager::CheckCounters(txn_holder, storage_)) {
    case VerifyMasterResult::VALID: {
      break;
    }
    case VerifyMasterResult::ABORT: {
      txn->set_status(TransactionStatus::ABORTED);
      will_abort = true;
      break;
    }
    case VerifyMasterResult::WAITING: {
      LOG(ERROR) << "Transaction " << txn_id << " was sent to worker with a high counter";
      break;
    }
    default:
      LOG(ERROR) << "Unrecognized check counter result";
      break;
  }

  PopulateDataFromLocalStorage(txn);

  // Send abort result and local reads to all remote active partitions
  Request request;
  auto local_partition = config_->GetLocalPartition();
  auto rrr = request.mutable_remote_read_result();
  rrr->set_txn_id(txn_id);
  rrr->set_partition(local_partition);
  rrr->set_will_abort(will_abort);
  if (!will_abort) {
    rrr->set_will_abort(false);
    auto reads_to_be_sent = rrr->mutable_reads();
    for (auto& key_value : txn->read_set()) {
      (*reads_to_be_sent)[key_value.first] = key_value.second;
    }
  }
  auto local_replica = config_->GetLocalReplica();
  for (auto p : txn_holder->ActivePartitions()) {
    if (p != local_partition) {
      auto machine_id = MakeMachineIdAsString(local_replica, p);
      SendToScheduler(request, std::move(machine_id));
    }
  }

  // If the txn does not do any write or does not have to wait for remote read, 
  // move on with executing this transaction.
  if (txn_holder->ActivePartitions().count(local_partition) == 0 || state.remote_reads_waiting_on == 0) {
    VLOG(3) << "Execute txn " << txn_id << " without remote reads";
    ExecuteAndCommitTransaction(txn_id);
  } else {
    VLOG(3) << "Defer executing txn " << txn->internal().id() << " until having enough remote reads";
  }
}

TransactionState& Worker::InitializeTransactionState(TransactionHolder* txn_holder) {
  auto txn = txn_holder->GetTransaction();
  auto txn_id = txn->internal().id();
  auto res = txn_states_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(txn_id),
      std::forward_as_tuple(txn_holder));

  CHECK(res.second) << "Transaction " << txn_id << " has already been dispatched to this worker";

  auto& state = txn_states_[txn_id];
  state.remote_reads_waiting_on = txn_holder->InvolvedPartitions().size() - 1;

  auto local_partition = config_->GetLocalPartition();
  vector<Key> key_vec;
    
  // Remove keys in remote partitions
  key_vec.reserve(txn->read_set_size());
  for (auto& key_value : *txn->mutable_read_set()) {
    const auto& key = key_value.first;
    auto partition = config_->GetPartitionOfKey(key);
    if (partition != local_partition) {
      key_vec.push_back(key);
    }
  }
  for (const auto& key : key_vec) {
    txn->mutable_read_set()->erase(key);
  }
  key_vec.reserve(txn->write_set_size());
  for (auto& key_value : *txn->mutable_write_set()) {
    const auto& key = key_value.first;
    auto partition = config_->GetPartitionOfKey(key);
    if (partition != local_partition) {
      key_vec.push_back(key);
    }
  }
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

  auto txn = txn_states_[txn_id].txn_holder->GetTransaction();

  txn_states_[txn_id].remote_reads_waiting_on -= 1;
  if (read_result.will_abort()) {
    txn->set_status(TransactionStatus::ABORTED);
  }
  // Apply remote reads to local txn
  for (const auto& key_value : read_result.reads()) {
    (*txn->mutable_read_set())[key_value.first] = key_value.second;
  }

  // If all remote reads arrived, move on with executing this transaction.
  if (txn_states_[txn_id].remote_reads_waiting_on == 0) {
    VLOG(3) << "Execute txn " << txn_id << " after receving all remote read results";
    ExecuteAndCommitTransaction(txn_id);
  }
}

void Worker::ExecuteAndCommitTransaction(TxnId txn_id) {
  const auto& state = txn_states_[txn_id];
  auto txn = state.txn_holder->GetTransaction();

  if (txn->status() != TransactionStatus::ABORTED) {
    ApplyWrites(txn_id);
  }

  // Response back to the scheduler
  Response res;
  res.mutable_worker()->set_txn_id(txn_id);

  RecordTxnEvent(
      config_,
      txn->mutable_internal(),
      TransactionEvent::EXIT_WORKER);

  SendToScheduler(res);

  // Done with this txn. Remove it from the state map
  txn_states_.erase(txn_id);
}

void Worker::ApplyWrites(TxnId txn_id) {
  const auto& state = txn_states_[txn_id];
  auto txn = state.txn_holder->GetTransaction();
  if (txn->procedure_case() == Transaction::ProcedureCase::kCode) {
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
  } else if (txn->procedure_case() == Transaction::ProcedureCase::kNewMaster) {
    const auto& key = txn->write_set().begin()->first;
    if (config_->KeyIsInLocalPartition(key)) {
      auto txn_key_metadata = txn->internal().master_metadata().at(key);
      Record record;
      bool found = storage_->Read(key, record);
      if (!found) {
        // TODO: handle case where key is deleted
        LOG(FATAL) << "Remastering key that does not exist: " << key;
      }
      record.metadata = Metadata(txn->new_master(), txn_key_metadata.counter() + 1);
      storage_->Write(key, record);
    }
    txn->set_status(TransactionStatus::COMMITTED);
  } else {
    LOG(ERROR) << "Procedure is not set";
  }
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