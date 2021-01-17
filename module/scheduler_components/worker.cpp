#include "module/scheduler_components/worker.h"

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
#include "module/scheduler_components/remaster_manager.h"
#endif /* defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY) */

#include <glog/logging.h>

#include <thread>

#include "common/monitor.h"
#include "common/proto_utils.h"
#include "module/scheduler.h"

namespace slog {

using internal::Envelope;
using internal::Request;
using internal::Response;

Worker::Worker(const ConfigurationPtr& config, const std::shared_ptr<Broker>& broker, Channel channel,
               const shared_ptr<Storage<Key, Record>>& storage, std::chrono::milliseconds poll_timeout)
    : NetworkedModule("Worker-" + std::to_string(channel), broker, channel, poll_timeout),
      config_(config),
      storage_(storage),
      // TODO: change this dynamically based on selected experiment
      commands_(new KeyValueCommands()) {}

void Worker::HandleInternalRequest(EnvelopePtr&& env) {
  auto& request = env->request();
  std::optional<TxnId> txn_id = {};
  bool valid_request = true;
  switch (request.type_case()) {
    case Request::kWorker: {
      txn_id = ProcessWorkerRequest(request.worker());
      break;
    }
    case Request::kRemoteReadResult: {
      txn_id = ProcessRemoteReadResult(request.remote_read_result());
      break;
    }
    default:
      valid_request = false;
      break;
  }
  if (valid_request) {
    if (txn_id) {
      AdvanceTransaction(*txn_id);
    }
  } else {
    LOG(FATAL) << "Invalid request for worker";
  }
}

std::optional<TxnId> Worker::ProcessWorkerRequest(const internal::WorkerRequest& worker_request) {
  auto txn_holder = reinterpret_cast<TxnHolder*>(worker_request.txn_holder_ptr());
  auto txn = txn_holder->transaction();
  auto txn_id = txn->internal().id();
  auto local_partition = config_->local_partition();

  TRACE(txn->mutable_internal(), TransactionEvent::ENTER_WORKER);

  // Create a state for the new transaction
  auto [iter, ok] = txn_states_.try_emplace(txn_id, txn_holder);

  DCHECK(ok) << "Transaction " << txn_id << " has already been dispatched to this worker";

  if (txn->status() == TransactionStatus::ABORTED) {
    NotifyOtherPartitions(txn_id);
    iter->second.phase = TransactionState::Phase::FINISH;
  } else {
    iter->second.phase = TransactionState::Phase::READ_LOCAL_STORAGE;
    // Remove keys that will be filled in later by remote partitions.
    // They are removed at this point so that the next phase will only
    // read the local keys from local storage.
    auto itr = txn->mutable_read_set()->begin();
    while (itr != txn->mutable_read_set()->end()) {
      const auto& key = itr->first;
      auto partition = config_->partition_of_key(key);
      if (partition != local_partition) {
        itr = txn->mutable_read_set()->erase(itr);
      } else {
        itr++;
      }
    }
    itr = txn->mutable_write_set()->begin();
    while (itr != txn->mutable_write_set()->end()) {
      const auto& key = itr->first;
      auto partition = config_->partition_of_key(key);
      if (partition != local_partition) {
        itr = txn->mutable_write_set()->erase(itr);
      } else {
        itr++;
      }
    }
  }

  // Establish a redirection at broker for this txn so that we can receive remote reads
  auto redirect_env = NewEnvelope();
  redirect_env->mutable_request()->mutable_broker_redirect()->set_tag(txn_id);
  redirect_env->mutable_request()->mutable_broker_redirect()->set_channel(channel());
  Send(move(redirect_env), kBrokerChannel);

  VLOG(3) << "Initialized state for txn " << txn_id;

  return txn_id;
}

std::optional<TxnId> Worker::ProcessRemoteReadResult(const internal::RemoteReadResult& read_result) {
  auto txn_id = read_result.txn_id();
  auto state_it = txn_states_.find(txn_id);
  if (state_it == txn_states_.end()) {
    VLOG(1) << "Transaction " << txn_id << " does not exist for remote read result";
    return {};
  }

  VLOG(2) << "Got remote read result for txn " << txn_id;

  auto& state = state_it->second;
  auto txn = state.txn_holder->transaction();

  if (txn->status() != TransactionStatus::ABORTED) {
    if (read_result.will_abort()) {
      // TODO: optimize by returning an aborting transaction to the scheduler immediately.
      // later remote reads will need to be garbage collected.
      txn->set_status(TransactionStatus::ABORTED);
      txn->set_abort_reason(read_result.abort_reason());
    } else {
      // Apply remote reads. After this point, the transaction has all the data it needs to
      // execute the code.
      for (const auto& key_value : read_result.reads()) {
        (*txn->mutable_read_set())[key_value.first] = key_value.second;
      }
    }
  }

  state.remote_reads_waiting_on -= 1;

  // Move the transaction to a new phase if all remote reads arrive
  if (state.remote_reads_waiting_on == 0) {
    if (state.phase == TransactionState::Phase::WAIT_REMOTE_READ) {
      state.phase = TransactionState::Phase::EXECUTE;
      VLOG(3) << "Execute txn " << txn_id << " after receving all remote read results";
    } else {
      LOG(FATAL) << "Invalid phase";
    }
  }

  return txn_id;
}

void Worker::AdvanceTransaction(TxnId txn_id) {
  auto& state = TxnState(txn_id);
  switch (state.phase) {
    case TransactionState::Phase::READ_LOCAL_STORAGE:
      ReadLocalStorage(txn_id);
      [[fallthrough]];
    case TransactionState::Phase::WAIT_REMOTE_READ:
      if (state.phase == TransactionState::Phase::WAIT_REMOTE_READ) {
        // The only way to get out of this phase is through remote messages
        break;
      }
      [[fallthrough]];
    case TransactionState::Phase::EXECUTE:
      if (state.phase == TransactionState::Phase::EXECUTE) {
        Execute(txn_id);
      }
      [[fallthrough]];
    case TransactionState::Phase::COMMIT:
      if (state.phase == TransactionState::Phase::COMMIT) {
        Commit(txn_id);
      }
      [[fallthrough]];
    case TransactionState::Phase::FINISH:
      Finish(txn_id);
      // Never fallthrough after this point because Finish and PreAbort
      // has already destroyed the state object
      break;
  }
}

void Worker::ReadLocalStorage(TxnId txn_id) {
  auto& state = TxnState(txn_id);
  auto txn_holder = state.txn_holder;
  auto txn = txn_holder->transaction();

  auto will_abort = false;

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
  switch (RemasterManager::CheckCounters(*txn_holder, storage_)) {
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
  // TODO: this loop can be merged with the one below to reduce accesses to
  //       the storage
  for (auto& key_pair : txn->internal().master_metadata()) {
    auto& key = key_pair.first;
    auto txn_master = key_pair.second.master();

    Record record;
    bool found = storage_->Read(key, record);
    if (found && txn_master != record.metadata.master) {
      will_abort = true;
      break;
    }
  }
#endif /* defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY) */

  if (will_abort) {
    txn->set_status(TransactionStatus::ABORTED);
    txn->set_abort_reason("Outdated master information");
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

  NotifyOtherPartitions(txn_id);

  // TODO: if will_abort == true, we can immediate jump to the FINISH phased.
  //       To do this, we need to removing the CHECK at the start of ProcessRemoteReadResult
  //       because we no longer require an aborted txn to receive all remote reads
  //       before moving on.
  // Set the number of remote reads that this partition needs to wait for
  state.remote_reads_waiting_on = 0;
  const auto& active_partitions = txn_holder->active_partitions();
  if (std::find(active_partitions.begin(), active_partitions.end(), config_->local_partition()) !=
      active_partitions.end()) {
    // Active partition needs remote reads from all partitions
    state.remote_reads_waiting_on = txn->internal().involved_partitions_size() - 1;
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
  auto& state = TxnState(txn_id);
  auto txn = state.txn_holder->transaction();

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
  auto& state = TxnState(txn_id);
  auto txn = state.txn_holder->transaction();
  switch (txn->procedure_case()) {
    case Transaction::ProcedureCase::kCode: {
      // Apply all writes to local storage if the transaction is not aborted
      if (txn->status() != TransactionStatus::COMMITTED) {
        VLOG(3) << "Txn " << txn_id << " aborted with reason: " << txn->abort_reason();
        break;
      }
      auto& master_metadata = txn->internal().master_metadata();
      for (const auto& key_value : txn->write_set()) {
        const auto& key = key_value.first;
        if (config_->key_is_in_local_partition(key)) {
          const auto& value = key_value.second;
          Record record;
          bool found = storage_->Read(key_value.first, record);
          if (!found) {
            DCHECK(master_metadata.contains(key)) << "Master metadata for key \"" << key << "\" is missing";
            record.metadata = master_metadata.at(key);
          }
          record.value = value;
          storage_->Write(key, record);
        }
      }
      for (const auto& key : txn->delete_set()) {
        if (config_->key_is_in_local_partition(key)) {
          storage_->Delete(key);
        }
      }
      VLOG(3) << "Committed txn " << txn_id;
      break;
    }
    case Transaction::ProcedureCase::kRemaster: {
      const auto& key = txn->write_set().begin()->first;
      if (config_->key_is_in_local_partition(key)) {
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
  TRACE(TxnState(txn_id).txn_holder->transaction()->mutable_internal(), TransactionEvent::EXIT_WORKER);

  // This must happen before the sending to scheduler below. Otherwise,
  // the scheduler may destroy the transaction holder before we can
  // send the transaction to the server.
  SendToCoordinatingServer(txn_id);

  // Notify the scheduler that we're done
  auto sched_env = NewEnvelope();
  sched_env->mutable_response()->mutable_worker()->set_txn_id(txn_id);
  Send(move(sched_env), kSchedulerChannel);

  // Remove the redirection at broker for this txn
  auto redirect_env = NewEnvelope();
  redirect_env->mutable_request()->mutable_broker_redirect()->set_tag(txn_id);
  redirect_env->mutable_request()->mutable_broker_redirect()->set_stop(true);
  Send(move(redirect_env), kBrokerChannel);

  // Done with this txn. Remove it from the state map
  txn_states_.erase(txn_id);

  VLOG(3) << "Finished with txn " << txn_id;
}

void Worker::NotifyOtherPartitions(TxnId txn_id) {
  auto& state = TxnState(txn_id);
  auto txn_holder = state.txn_holder;

  if (txn_holder->active_partitions().empty()) {
    return;
  }

  auto txn = txn_holder->transaction();
  auto local_partition = config_->local_partition();
  auto local_replica = config_->local_replica();
  auto aborted = txn->status() == TransactionStatus::ABORTED;

  // Send abort result and local reads to all remote active partitions
  Envelope env;
  auto rrr = env.mutable_request()->mutable_remote_read_result();
  rrr->set_txn_id(txn_id);
  rrr->set_partition(local_partition);
  rrr->set_will_abort(aborted);
  rrr->set_abort_reason(txn->abort_reason());
  if (!aborted) {
    auto reads_to_be_sent = rrr->mutable_reads();
    for (auto& key_value : txn->read_set()) {
      (*reads_to_be_sent)[key_value.first] = key_value.second;
    }
  }

  for (auto p : txn_holder->active_partitions()) {
    if (p != local_partition) {
      auto machine_id = config_->MakeMachineId(local_replica, p);
      Send(env, std::move(machine_id), txn_id);
    }
  }
}

void Worker::SendToCoordinatingServer(TxnId txn_id) {
  auto& state = TxnState(txn_id);
  auto txn_holder = state.txn_holder;

  // Send the txn back to the coordinating server
  Envelope env;
  auto completed_sub_txn = env.mutable_request()->mutable_completed_subtxn();
  completed_sub_txn->set_partition(config_->local_partition());

  Transaction* txn = txn_holder->transaction();
  if (!config_->return_dummy_txn()) {
    completed_sub_txn->set_allocated_txn(txn);
  } else {
    auto dummy_txn = completed_sub_txn->mutable_txn();
    dummy_txn->set_status(txn->status());
    dummy_txn->set_abort_reason(txn->abort_reason());
    dummy_txn->mutable_internal()->set_id(txn->internal().id());
    dummy_txn->mutable_internal()->set_type(txn->internal().type());
    dummy_txn->mutable_internal()->set_coordinating_server(txn->internal().coordinating_server());
    dummy_txn->mutable_internal()->mutable_involved_partitions()->CopyFrom(txn->internal().involved_partitions());
    dummy_txn->mutable_internal()->mutable_events()->CopyFrom(txn->internal().events());
    dummy_txn->mutable_internal()->mutable_event_times()->CopyFrom(txn->internal().event_times());
    dummy_txn->mutable_internal()->mutable_event_machines()->CopyFrom(txn->internal().event_machines());
  }

  Send(env, txn->internal().coordinating_server(), kServerChannel);

  if (!config_->return_dummy_txn()) {
    completed_sub_txn->release_txn();
  }
}

TransactionState& Worker::TxnState(TxnId txn_id) {
  auto state_it = txn_states_.find(txn_id);
  DCHECK(state_it != txn_states_.end());
  return state_it->second;
}

}  // namespace slog