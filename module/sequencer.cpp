#include "module/sequencer.h"

#include <glog/logging.h>

#include "common/monitor.h"
#include "common/proto_utils.h"
#include "module/ticker.h"
#include "paxos/simple_multi_paxos.h"

using std::move;

namespace slog {

using internal::Batch;
using internal::Request;
using internal::Response;

Sequencer::Sequencer(const ConfigurationPtr& config, const std::shared_ptr<Broker>& broker, milliseconds batch_timeout,
                     milliseconds poll_timeout)
    : NetworkedModule("Sequencer", broker, kSequencerChannel, poll_timeout),
      config_(config),
      batch_timeout_(batch_timeout),
      batch_id_counter_(0) {
  NewBatch();
}

void Sequencer::NewBatch() {
  if (batch_ == nullptr) {
    batch_.reset(new Batch());
  }
  batch_->Clear();
  batch_->set_transaction_type(TransactionType::SINGLE_HOME);
  ++batch_id_counter_;
  batch_->set_id(batch_id_counter_ * kMaxNumMachines + config_->local_machine_id());
}

void Sequencer::HandleInternalRequest(EnvelopePtr&& env) {
  switch (env->request().type_case()) {
    case Request::kForwardTxn: {
      // Received a single-home txn
      auto txn = env->mutable_request()->mutable_forward_txn()->release_txn();

      TRACE(txn->mutable_internal(), TransactionEvent::ENTER_SEQUENCER);

      ScheduleBatch(txn);
      break;
    }
    case Request::kForwardBatch: {
      // Received a batch of multi-home txns
      if (env->request().forward_batch().part_case() == internal::ForwardBatch::kBatchData) {
        ProcessMultiHomeBatch(move(env));
      }
      break;
    }
    default:
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(env->request().type_case(), Request) << "\"";
      break;
  }
}

void Sequencer::ProcessMultiHomeBatch(EnvelopePtr&& env) {
  auto batch = env->mutable_request()->mutable_forward_batch()->mutable_batch_data();
  if (batch->transaction_type() != TransactionType::MULTI_HOME) {
    LOG(ERROR) << "Batch has to contain multi-home txns";
    return;
  }

  TRACE(batch, TransactionEvent::ENTER_SEQUENCER_IN_BATCH);

  auto local_rep = config_->local_replica();
  // For each multi-home txn, create a lock-only txn and put into
  // the single-home batch to be sent to the local log
  for (auto& txn : batch->transactions()) {
    auto lock_only_txn = new Transaction();

    const auto& metadata = txn.internal().master_metadata();
    auto lock_only_metadata = lock_only_txn->mutable_internal()->mutable_master_metadata();

    // Copy keys and metadata in local replica
    for (auto& key_value : txn.read_set()) {
      auto master = metadata.at(key_value.first).master();
      if (master == local_rep) {
        lock_only_txn->mutable_read_set()->insert(key_value);
        lock_only_metadata->insert({key_value.first, metadata.at(key_value.first)});
      }
    }
    for (auto& key_value : txn.write_set()) {
      auto master = metadata.at(key_value.first).master();
      if (master == local_rep) {
        lock_only_txn->mutable_write_set()->insert(key_value);
        lock_only_metadata->insert({key_value.first, metadata.at(key_value.first)});
      }
    }

#ifdef REMASTER_PROTOCOL_COUNTERLESS
    // Add additional lock only at new replica
    // TODO: refactor to remote metadata from lock-onlys. Requires
    // changes in the scheduler
    if (txn.procedure_case() == Transaction::kRemaster) {
      lock_only_txn->mutable_remaster()->set_new_master((txn.remaster().new_master()));
      if (txn.remaster().new_master() == local_rep) {
        lock_only_txn->CopyFrom(txn);
        lock_only_txn->mutable_remaster()->set_is_new_master_lock_only(true);
      }
    }
#endif /* REMASTER_PROTOCOL_COUNTERLESS */

    lock_only_txn->mutable_internal()->set_id(txn.internal().id());
    lock_only_txn->mutable_internal()->set_type(TransactionType::LOCK_ONLY);

    if (!lock_only_txn->read_set().empty() || !lock_only_txn->write_set().empty()) {
      ScheduleBatch(lock_only_txn);
    }
  }

  TRACE(batch, TransactionEvent::EXIT_SEQUENCER_IN_BATCH);

  // Replicate the batch of multi-home txns to all machines in the same region
  auto num_partitions = config_->num_partitions();
  for (uint32_t part = 0; part < num_partitions; part++) {
    auto machine_id = config_->MakeMachineId(local_rep, part);
    Send(*env, machine_id, kInterleaverChannel);
  }
}

void Sequencer::ScheduleBatch(Transaction* txn) {
  DCHECK(txn->internal().type() == TransactionType::SINGLE_HOME || txn->internal().type() == TransactionType::LOCK_ONLY)
      << "Sequencer batch can only contain single-home or lock-only txn. "
      << "Multi-home txn or unknown txn type received instead.";

  batch_->mutable_transactions()->AddAllocated(txn);
  // If this is the first txn of the batch, start the timer to send the batch
  if (batch_->transactions_size() == 1) {
    NewTimedCallback(batch_timeout_, [this]() {
      SendBatch();
      NewBatch();
    });
  }
}

void Sequencer::SendBatch() {
  VLOG(3) << "Finished batch " << batch_->id() << " of size " << batch_->transactions().size()
              << ". Sending out for ordering and replicating";

  auto paxos_env = NewEnvelope();
  auto paxos_propose = paxos_env->mutable_request()->mutable_paxos_propose();
  paxos_propose->set_value(config_->local_partition());
  Send(move(paxos_env), kLocalPaxos);

  auto batch_env = NewEnvelope();
  auto forward_batch = batch_env->mutable_request()->mutable_forward_batch();
  // Minus 1 so that batch id counter starts from 0
  forward_batch->set_same_origin_position(batch_id_counter_ - 1);
  forward_batch->set_allocated_batch_data(batch_.release());

  TRACE(forward_batch->mutable_batch_data(), TransactionEvent::EXIT_SEQUENCER_IN_BATCH);

  if (!SendBatchDelayed(batch_env)) {
    auto num_partitions = config_->num_partitions();
    auto num_replicas = config_->num_replicas();
    for (uint32_t part = 0; part < num_partitions; part++) {
      for (uint32_t rep = 0; rep < num_replicas; rep++) {
        auto machine_id = config_->MakeMachineId(rep, part);
        if (machine_id != config_->local_machine_id()) {
          Send(*batch_env, machine_id, kInterleaverChannel);
        }
      }
    }
  }
  // Forward to the next module in the same machine
  Send(move(batch_env), kInterleaverChannel);
}

bool Sequencer::SendBatchDelayed(const EnvelopePtr& env) {
  if (!config_->replication_delay_pct()) {
    return false;
  }

  std::bernoulli_distribution is_delayed(config_->replication_delay_pct() / 100.0);
  if (!is_delayed(rg_)) {
    return false;
  }

  auto local_rep = config_->local_replica();
  auto num_partitions = config_->num_partitions();
  for (uint32_t part = 0; part < num_partitions; part++) {
    auto machine_id = config_->MakeMachineId(local_rep, part);
    Send(*env, machine_id, kInterleaverChannel);
  }
  
  auto delay_ms = config_->replication_delay_amount_ms();

  VLOG(4) << "Delay batch " << env->request().forward_batch().batch_data().id() << " for " << delay_ms << " ms";

  NewTimedCallback(milliseconds(delay_ms), [this, delayed_batch = internal::Envelope(*env)]() {
    VLOG(4) << "Sending delayed batch " << delayed_batch.request().forward_batch().batch_data().id();
    // Replicate batch to all machines EXCEPT local replica
    auto num_replicas = config_->num_replicas();
    auto num_partitions = config_->num_partitions();
    for (uint32_t rep = 0; rep < num_replicas; rep++) {
      // Already sent to local replica
      if (rep != config_->local_replica()) {
        for (uint32_t part = 0; part < num_partitions; part++) {
          auto machine_id = config_->MakeMachineId(rep, part);
          Send(delayed_batch, machine_id, kInterleaverChannel);
        }
      }
    }
  });

  return true;
}

}  // namespace slog