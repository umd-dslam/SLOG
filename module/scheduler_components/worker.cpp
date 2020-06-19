#include "module/scheduler_components/worker.h"

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
#include "module/scheduler_components/remaster_manager.h"
#endif /* defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY) */

#include <thread>

#include "common/proto_utils.h"
#include "module/scheduler.h"

#ifdef __GNUC__
#define FALLTHROUGH [[gnu::fallthrough]]
#else
#define FALLTHROUGH
#endif

namespace slog {

using internal::Request;
using internal::Response;

Worker::Worker(
    const string& identity,
    const ConfigurationPtr& config,
    zmq::context_t& context,
    const shared_ptr<Storage<Key, Record>>& storage)
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
    TxnId txn_id = 0;
    bool valid_request = true;
    switch (req.type_case()) {
      case Request::kWorker: {
        txn_id = ProcessWorkerRequest(req.worker());
        break;
      }
      case Request::kRemoteReadResult: {
        txn_id = ProcessRemoteReadResult(req.remote_read_result());
        break;
      }
      default:
        valid_request = false;
        break;
    }
    if (valid_request) {
      AdvanceTransaction(txn_id);
    } else {
      LOG(FATAL) << "Invalid request for worker";
    }
  }
  VLOG_EVERY_N(4, 5000/MODULE_POLL_TIMEOUT_MS) << "Worker " << identity_ << " is alive.";
}

TxnId Worker::ProcessWorkerRequest(const internal::WorkerRequest& worker_request) {
  auto txn_holder = reinterpret_cast<TransactionHolder*>(worker_request.txn_holder_ptr());
  auto txn = txn_holder->GetTransaction();
  auto txn_id = txn->internal().id();
  auto local_partition = config_->GetLocalPartition();

  RecordTxnEvent(
      config_,
      txn->mutable_internal(),
      TransactionEvent::ENTER_WORKER);

  // Remove keys that will be filled in later by remote partitions.
  // They are removed at this point so that the next phase will only
  // read the local keys from local storage.
  auto itr = txn->mutable_read_set()->begin();
  while (itr != txn->mutable_read_set()->end()) {
    const auto& key = itr->first;
    auto partition = config_->GetPartitionOfKey(key);
    if (partition != local_partition) {
      itr = txn->mutable_read_set()->erase(itr);
    } else {
      itr++;
    }
  }
  itr = txn->mutable_write_set()->begin();
  while (itr != txn->mutable_write_set()->end()) {
    const auto& key = itr->first;
    auto partition = config_->GetPartitionOfKey(key);
    if (partition != local_partition) {
      itr = txn->mutable_write_set()->erase(itr);
    } else {
      itr++;
    }
  }

  // Create a state for the new transaction
  auto state = txn_states_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(txn_id),
      std::forward_as_tuple(txn_holder));

  CHECK(state.second) << "Transaction " << txn_id << " has already been dispatched to this worker";

  VLOG(3) << "Initialized state for txn " << txn_id;

  return txn_id;
}

TxnId Worker::ProcessRemoteReadResult(const internal::RemoteReadResult& read_result) {
  auto txn_id = read_result.txn_id();

  CHECK(txn_states_.count(txn_id) > 0)
      << "Transaction " << txn_id << " does not exist for remote read result";

  auto& state = txn_states_[txn_id];
  auto txn = state.txn_holder->GetTransaction();

  if (read_result.will_abort()) {
    // TODO: optimize by returning an aborting transaction to the scheduler immediately.
    // later remote reads will need to be garbage collected.
    txn->set_status(TransactionStatus::ABORTED);
  } else {
    // Apply remote reads. After this point, the transaction has all the data it needs to
    // execute the code.
    for (const auto& key_value : read_result.reads()) {
      (*txn->mutable_read_set())[key_value.first] = key_value.second;
    }
  }

  state.remote_reads_waiting_on -= 1;

  // Move the transaction to a new phase if all remote reads arrive
  if (state.remote_reads_waiting_on == 0) {
    if (state.phase == TransactionState::Phase::WAIT_REMOTE_READ) {
      state.phase = TransactionState::Phase::EXECUTE;
      VLOG(3) << "Execute txn " << txn_id << " after receving all remote read results";
    }
    else {
      LOG(FATAL) << "Invalid phase";
    }
  }

  return txn_id;
}

void Worker::AdvanceTransaction(TxnId txn_id) {
  auto& state = txn_states_[txn_id];
  switch (state.phase) {
    case TransactionState::Phase::READ_LOCAL_STORAGE:
      ReadLocalStorage(txn_id);
      FALLTHROUGH;
    case TransactionState::Phase::WAIT_REMOTE_READ:
      if (state.phase == TransactionState::Phase::WAIT_REMOTE_READ) {
        // The only way to get out of this phase is through remote messages
        break;
      }
      FALLTHROUGH;
    case TransactionState::Phase::EXECUTE:
      if (state.phase == TransactionState::Phase::EXECUTE) {
        Execute(txn_id);
      }
      FALLTHROUGH;
    case TransactionState::Phase::COMMIT:
      if (state.phase == TransactionState::Phase::COMMIT) {
        Commit(txn_id);
      }
      FALLTHROUGH;
    case TransactionState::Phase::FINISH:
      if (state.phase == TransactionState::Phase::FINISH) {
        Finish(txn_id);
      }
  }
}

void Worker::ReadLocalStorage(TxnId txn_id) {
  auto& state = txn_states_[txn_id];
  auto txn_holder = state.txn_holder;
  auto txn = txn_holder->GetTransaction();

  auto will_abort = false;

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
  switch(RemasterManager::CheckCounters(txn_holder, storage_)) {
    case VerifyMasterResult::VALID: {
      break;
    }
    case VerifyMasterResult::ABORT: {
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
#else
  // Check whether the store master metadata matches with the information
  // stored in the transaction
  // TODO: this loop can be merged with the one below to avoid
  // duplicate access to the storage
  for (auto& key_pair : txn->internal().master_metadata()) {
    auto& key = key_pair.first;
    auto txn_master = key_pair.second.master();

    Record record;
    bool found = storage_->Read(key, record);
    if (found) {
      if (txn_master != record.metadata.master) {
        will_abort = true;
        break;
      }
    }
  }
#endif /* defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY) */

  if (will_abort) {
    txn->set_status(TransactionStatus::ABORTED);
  } else {
    // If not abort due to remastering, read from local storage
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

  // Send abort result and local reads to all remote active partitions
  Request request;
  auto local_partition = config_->GetLocalPartition();
  auto rrr = request.mutable_remote_read_result();
  rrr->set_txn_id(txn_id);
  rrr->set_partition(local_partition);
  rrr->set_will_abort(will_abort);
  if (!will_abort) {
    auto reads_to_be_sent = rrr->mutable_reads();
    for (auto& key_value : txn->read_set()) {
      (*reads_to_be_sent)[key_value.first] = key_value.second;
    }
  }
  SendToOtherPartitions(std::move(request), txn_holder->ActivePartitions());

  // TODO: if will_abort == true, we can immediate jump to the FINISH phased.
  //       To do this, we need to removing the CHECK at the start of ProcessRemoteReadResult
  //       because we no longer require an aborted txn to receive all remote reads
  //       before moving on.
  // Set the number of remote reads that this partition needs to wait for
  state.remote_reads_waiting_on = 0;
  if (txn_holder->ActivePartitions().count(local_partition) > 0) {
    // Active partition needs remote reads from all partitions
    state.remote_reads_waiting_on = txn_holder->InvolvedPartitions().size() - 1;
  }
  if (state.remote_reads_waiting_on == 0) {
    VLOG(3) << "Execute txn " << txn_id << " without remote reads";
    state.phase = TransactionState::Phase::EXECUTE;
  } else {
    VLOG(3) << "Defer executing txn " << txn_id << " until having enough remote reads";
    state.phase = TransactionState::Phase::WAIT_REMOTE_READ;
  }
}

void Worker::Execute(TxnId txn_id) {
  auto& state = txn_states_[txn_id];
  auto txn = state.txn_holder->GetTransaction();

  switch (txn->procedure_case()) {
    case Transaction::ProcedureCase::kCode: {
      if (txn->status() == TransactionStatus::ABORTED) {
        break;
      }
      // Execute the transaction code
      commands_->Execute(*txn);
      break;
    }
    case Transaction::ProcedureCase::kRemaster:
      txn->set_status(TransactionStatus::COMMITTED);
      break;
    default:
      LOG(FATAL) << "Procedure is not set";
  }
  state.phase = TransactionState::Phase::COMMIT;
}

void Worker::Commit(TxnId txn_id) {
  auto& state = txn_states_[txn_id];
  auto txn = state.txn_holder->GetTransaction();
  switch (txn->procedure_case()) {
    case Transaction::ProcedureCase::kCode: {
      // Apply all writes to local storage if the transaction is not aborted
      if (txn->status() != TransactionStatus::COMMITTED) {
        break;
      }
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
      break;
    }
    case Transaction::ProcedureCase::kRemaster: {
      const auto& key = txn->write_set().begin()->first;
      if (config_->KeyIsInLocalPartition(key)) {
        auto txn_key_metadata = txn->internal().master_metadata().at(key);
        Record record;
        bool found = storage_->Read(key, record);
        if (!found) {
          // TODO: handle case where key is deleted
          LOG(FATAL) << "Remastering key that does not exist: " << key;
        }
        record.metadata = Metadata(txn->remaster().new_master(), txn_key_metadata.counter() + 1);
        storage_->Write(key, record);
      }
      break;
    }
    default:
      LOG(FATAL) << "Procedure is not set";
  }
  state.phase = TransactionState::Phase::FINISH;
}

void Worker::Finish(TxnId txn_id) {
  auto txn = txn_states_[txn_id].txn_holder->GetTransaction();
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

  VLOG(3) << "Finished with txn " << txn_id;
}

void Worker::SendToOtherPartitions(
    internal::Request&& request,
    const std::unordered_set<uint32_t>& partitions) {
  auto local_replica = config_->GetLocalReplica();
  auto local_partition = config_->GetLocalPartition();
  for (auto p : partitions) {
    if (p != local_partition) {
      auto machine_id = MakeMachineIdAsString(local_replica, p);
      SendToScheduler(request, std::move(machine_id));
    }
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