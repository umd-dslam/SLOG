#include "module/scheduler_components/worker.h"

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
#include "module/scheduler_components/remaster_manager.h"
#endif /* defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY) */

#include <thread>
#include <glog/logging.h>

#include "common/proto_utils.h"
#include "module/scheduler.h"

namespace slog {

using internal::Request;
using internal::Response;

Worker::Worker(
    const ConfigurationPtr& config,
    const std::shared_ptr<Broker>& broker,
    Channel channel,
    const shared_ptr<Storage<Key, Record>>& storage)
  : NetworkedModule(broker, channel),
    config_(config),
    storage_(storage),
    // TODO: change this dynamically based on selected experiment
    commands_(new KeyValueCommands()) {}

void Worker::HandleInternalRequest(ReusableRequest&& req, MachineId) {
  std::optional<TxnId> txn_id = {};
  bool valid_request = true;
  switch (req.get()->type_case()) {
    case Request::kWorker: {
      txn_id = ProcessWorkerRequest(req.get()->worker());
      break;
    }
    case Request::kRemoteReadResult: {
      txn_id = ProcessRemoteReadResult(req.get()->remote_read_result());
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
  auto txn_holder = reinterpret_cast<TransactionHolder*>(worker_request.txn_holder_ptr());
  auto txn = txn_holder->transaction();
  auto txn_id = txn->internal().id();
  auto local_partition = config_->local_partition();

  RecordTxnEvent(
      config_,
      txn->mutable_internal(),
      TransactionEvent::ENTER_WORKER);

  // Create a state for the new transaction
  auto [iter, ok] = txn_states_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(txn_id),
      std::forward_as_tuple(txn_holder));

  CHECK(ok) << "Transaction " << txn_id << " has already been dispatched to this worker";

  if (txn->status() == TransactionStatus::ABORTED) {
    iter->second.phase = TransactionState::Phase::PRE_ABORT;
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

  VLOG(3) << "Initialized state for txn " << txn_id;

  return txn_id;
}

std::optional<TxnId> Worker::ProcessRemoteReadResult(const internal::RemoteReadResult& read_result) {
  auto txn_id = read_result.txn_id();
  if (txn_states_.count(txn_id) == 0) {
    VLOG(1) << "Transaction " << txn_id << " does not exist for remote read result";
    return {};
  }

  auto& state = txn_states_[txn_id];
  auto txn = state.txn_holder->transaction();

  if (txn->status() != TransactionStatus::ABORTED) {
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
    case TransactionState::Phase::PRE_ABORT:
      if (state.phase == TransactionState::Phase::FINISH) {
        Finish(txn_id);
      } else if (state.phase == TransactionState::Phase::PRE_ABORT) {
        PreAbort(txn_id);
      }
      // Never fallthrough after this point because Finish and PreAbort
      // has already destroyed the state object
      break;
  }
}

void Worker::ReadLocalStorage(TxnId txn_id) {
  auto& state = txn_states_[txn_id];
  auto txn_holder = state.txn_holder;
  auto txn = txn_holder->transaction();

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

  NotifyOtherPartitions(txn_id);

  // TODO: if will_abort == true, we can immediate jump to the FINISH phased.
  //       To do this, we need to removing the CHECK at the start of ProcessRemoteReadResult
  //       because we no longer require an aborted txn to receive all remote reads
  //       before moving on.
  // Set the number of remote reads that this partition needs to wait for
  state.remote_reads_waiting_on = 0;
  if (txn_holder->active_partitions().count(config_->local_partition()) > 0) {
    // Active partition needs remote reads from all partitions
    state.remote_reads_waiting_on = txn_holder->involved_partitions().size() - 1;
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
  auto& state = txn_states_[txn_id];
  auto txn = state.txn_holder->transaction();
  switch (txn->procedure_case()) {
    case Transaction::ProcedureCase::kCode: {
      // Apply all writes to local storage if the transaction is not aborted
      if (txn->status() != TransactionStatus::COMMITTED) {
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
            CHECK(master_metadata.contains(key))
                << "Master metadata for key \"" << key << "\" is missing";
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
  auto txn = txn_states_[txn_id].txn_holder->transaction();

  RecordTxnEvent(
      config_,
      txn->mutable_internal(),
      TransactionEvent::EXIT_WORKER);

  // This must happen before the sending to scheduler below. Otherwise,
  // the scheduler may destroy the transaction holder before we can
  // send the transaction to the server.
  SendToCoordinatingServer(txn_id);

  // Notify the scheduler that we're done
  auto res = AcquireResponse();
  res.get()->mutable_worker()->set_txn_id(txn_id);
  Send(*res.get(), kSchedulerChannel);

  // Done with this txn. Remove it from the state map
  txn_states_.erase(txn_id);

  VLOG(3) << "Finished with txn " << txn_id;
}

void Worker::PreAbort(TxnId txn_id) {
  NotifyOtherPartitions(txn_id);

  auto& state = txn_states_[txn_id];
  auto txn = state.txn_holder->transaction();

  RecordTxnEvent(
      config_,
      txn->mutable_internal(),
      TransactionEvent::EXIT_WORKER);

  SendToCoordinatingServer(txn_id);

  // The worker owns this holder for the case of pre-
  // aborted txn so it must delete it
  delete state.txn_holder;
  state.txn_holder = nullptr;

  txn_states_.erase(txn_id);

  VLOG(3) << "Finished with txn " << txn_id;
}

void Worker::NotifyOtherPartitions(TxnId txn_id) {
  auto& state = txn_states_[txn_id];
  auto txn_holder = state.txn_holder;

  if (txn_holder->active_partitions().empty()) {
    return;
  }

  auto txn = txn_holder->transaction();
  auto local_partition = config_->local_partition();
  auto local_replica = config_->local_replica();
  auto aborted = txn->status() == TransactionStatus::ABORTED;

  // Send abort result and local reads to all remote active partitions
  auto request = AcquireRequest();
  auto rrr = request.get()->mutable_remote_read_result();
  rrr->set_txn_id(txn_id);
  rrr->set_partition(local_partition);
  rrr->set_will_abort(aborted);
  if (!aborted) {
    auto reads_to_be_sent = rrr->mutable_reads();
    for (auto& key_value : txn->read_set()) {
      (*reads_to_be_sent)[key_value.first] = key_value.second;
    }
  }

  for (auto p : txn_holder->active_partitions()) {
    if (p != local_partition) {
      auto machine_id = config_->MakeMachineId(local_replica, p);
      Send(*request.get(), kSchedulerChannel, std::move(machine_id));
    }
  }
}

void Worker::SendToCoordinatingServer(TxnId txn_id) {
  auto& state = txn_states_[txn_id];
  auto txn_holder = state.txn_holder;
  auto txn = txn_holder->transaction();

  // Send the txn back to the coordinating server
  auto req = AcquireRequest();
  auto completed_sub_txn = req.get()->mutable_completed_subtxn();
  completed_sub_txn->set_allocated_txn(txn);
  completed_sub_txn->set_partition(config_->local_partition());
  for (auto p : txn_holder->involved_partitions()) {
    completed_sub_txn->add_involved_partitions(p);
  }
  
  Send(*req.get(), kServerChannel,  txn->internal().coordinating_server());
  completed_sub_txn->release_txn();
}

} // namespace slog