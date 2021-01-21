#include "module/sequencer.h"

#include <glog/logging.h>

#include <algorithm>

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
  partitioned_batch_.resize(config_->num_partitions());
  NewBatch();
}

void Sequencer::NewBatch() {
  ++batch_id_counter_;
  batch_scheduled_ = false;
  batch_size_ = 0;
  for (auto& batch : partitioned_batch_) {
    if (batch == nullptr) {
      batch.reset(new Batch());
    }
    batch->Clear();
    batch->set_transaction_type(TransactionType::SINGLE_HOME);
    batch->set_id(batch_id());
  }
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

  vector<internal::Envelope> partitioned_mh_batch;
  partitioned_mh_batch.resize(config_->num_partitions());
  // For each multi-home txn, create a lock-only txn and put into the single-home batch to be sent to the local log.
  // The txns are also sorted to corresponding partitions
  for (auto& txn : batch->transactions()) {
    if (auto lock_only_txn = GenerateLockOnlyTxn(txn); lock_only_txn) {
      ScheduleBatch(lock_only_txn);
    }
    for (auto p : txn.internal().involved_partitions()) {
      auto forward_batch = partitioned_mh_batch[p].mutable_request()->mutable_forward_batch();
      forward_batch->mutable_batch_data()->add_transactions()->CopyFrom(txn);
    }
  }

  TRACE(batch, TransactionEvent::EXIT_SEQUENCER_IN_BATCH);

  // Replicate the batch of multi-home txns to machines in the same region
  for (uint32_t part = 0; part < config_->num_partitions(); part++) {
    auto subbatch = partitioned_mh_batch[part].mutable_request()->mutable_forward_batch()->mutable_batch_data();

    subbatch->set_id(batch->id());
    subbatch->set_transaction_type(batch->transaction_type());
    subbatch->mutable_events()->CopyFrom(batch->events());
    subbatch->mutable_event_times()->CopyFrom(batch->event_times());
    subbatch->mutable_event_machines()->CopyFrom(batch->event_machines());

    auto machine_id = config_->MakeMachineId(config_->local_replica(), part);
    Send(partitioned_mh_batch[part], machine_id, kInterleaverChannel);
  }
}

void Sequencer::ScheduleBatch(Transaction* txn) {
  DCHECK(config_->bypass_mh_orderer() || txn->internal().type() == TransactionType::SINGLE_HOME ||
         txn->internal().type() == TransactionType::LOCK_ONLY)
      << "Sequencer batch can only contain single-home or lock-only txn. "
      << "Multi-home txn or unknown txn type received instead.";

  if (txn->internal().type() == TransactionType::MULTI_HOME) {
    auto lock_only_txn = GenerateLockOnlyTxn(*txn);

    delete txn;

    if (!lock_only_txn) {
      return;
    }
    txn = lock_only_txn;
  }

  for (auto p : txn->internal().involved_partitions()) {
    partitioned_batch_[p]->add_transactions()->CopyFrom(*txn);
  }
  ++batch_size_;

  if (!batch_scheduled_) {
    NewTimedCallback(batch_timeout_, [this]() {
      SendBatch();
      NewBatch();
    });
    batch_scheduled_ = true;
  }
}

void Sequencer::SendBatch() {
  VLOG(3) << "Finished batch " << batch_id() << " of size " << batch_size_
          << ". Sending out for ordering and replicating";

  auto paxos_env = NewEnvelope();
  auto paxos_propose = paxos_env->mutable_request()->mutable_paxos_propose();
  paxos_propose->set_value(config_->local_partition());
  Send(move(paxos_env), kLocalPaxos);

  if (!SendBatchDelayed()) {
    auto num_partitions = config_->num_partitions();
    auto num_replicas = config_->num_replicas();
    for (uint32_t part = 0; part < num_partitions; part++) {
      auto env = NewBatchRequest(partitioned_batch_[part].release());

      TRACE(env->mutable_request()->mutable_forward_batch()->mutable_batch_data(),
            TransactionEvent::EXIT_SEQUENCER_IN_BATCH);

      vector<MachineId> destinations;
      for (uint32_t rep = 0; rep < num_replicas; rep++) {
        destinations.push_back(config_->MakeMachineId(rep, part));
      }
      Send(move(env), destinations, kInterleaverChannel);
    }
  }
}

bool Sequencer::SendBatchDelayed() {
  if (!config_->replication_delay_pct()) {
    return false;
  }

  std::bernoulli_distribution is_delayed(config_->replication_delay_pct() / 100.0);
  if (!is_delayed(rg_)) {
    return false;
  }

  auto delay_ms = config_->replication_delay_amount_ms();

  VLOG(4) << "Delay batch " << batch_id() << " for " << delay_ms << " ms";

  for (uint32_t part = 0; part < config_->num_partitions(); part++) {
    auto env = NewBatchRequest(partitioned_batch_[part].release());

    // Send to the partition in the local replica immediately
    Send(*env, config_->MakeMachineId(config_->local_replica(), part), kInterleaverChannel);

    NewTimedCallback(milliseconds(delay_ms), [this, part, delayed_env = env.release()]() {
      VLOG(4) << "Sending delayed batch " << delayed_env->request().forward_batch().batch_data().id();
      // Replicate batch to all replicas EXCEPT local replica
      vector<MachineId> destinations;
      for (uint32_t rep = 0; rep < config_->num_replicas(); rep++) {
        if (rep != config_->local_replica()) {
          destinations.push_back(config_->MakeMachineId(rep, part));
        }
      }
      Send(*delayed_env, destinations, kInterleaverChannel);
      delete delayed_env;
    });
  }

  return true;
}

EnvelopePtr Sequencer::NewBatchRequest(internal::Batch* batch) {
  auto env = NewEnvelope();
  auto forward_batch = env->mutable_request()->mutable_forward_batch();
  // Minus 1 so that batch id counter starts from 0
  forward_batch->set_same_origin_position(batch_id_counter_ - 1);
  forward_batch->set_allocated_batch_data(batch);
  return env;
}

// Return null if input txn is not multi-home or the resulting lock-only txn does not
// contains any key
Transaction* Sequencer::GenerateLockOnlyTxn(const Transaction& txn) const {
  if (txn.internal().type() != TransactionType::MULTI_HOME) {
    return nullptr;
  }
  auto local_rep = config_->local_replica();
  auto lock_only_txn = new Transaction();
  const auto& metadata = txn.internal().master_metadata();
  auto lock_only_metadata = lock_only_txn->mutable_internal()->mutable_master_metadata();
  vector<uint32_t> involved_partitions;
  // Copy keys and metadata in local replica
  for (auto& key_value : txn.read_set()) {
    auto master = metadata.at(key_value.first).master();
    if (master == local_rep) {
      lock_only_txn->mutable_read_set()->insert(key_value);
      lock_only_metadata->insert({key_value.first, metadata.at(key_value.first)});
      involved_partitions.push_back(config_->partition_of_key(key_value.first));
    }
  }
  for (auto& key_value : txn.write_set()) {
    auto master = metadata.at(key_value.first).master();
    if (master == local_rep) {
      lock_only_txn->mutable_write_set()->insert(key_value);
      lock_only_metadata->insert({key_value.first, metadata.at(key_value.first)});
      involved_partitions.push_back(config_->partition_of_key(key_value.first));
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
      involved_partitions.clear();
      for (auto p : lock_only_txn->internal().involved_partitions()) {
        involved_partitions.push_back(p);
      }
    }
  }
#endif /* REMASTER_PROTOCOL_COUNTERLESS */

  if (lock_only_txn->read_set().empty() && lock_only_txn->write_set().empty()) {
    return nullptr;
  }

  // We need to recompute involved partitions because a lock-only txn only holds a subset of
  // the keys of the original txn.
  std::sort(involved_partitions.begin(), involved_partitions.end());
  auto last = std::unique(involved_partitions.begin(), involved_partitions.end());
  *lock_only_txn->mutable_internal()->mutable_involved_partitions() = {involved_partitions.begin(), last};

  lock_only_txn->mutable_internal()->set_id(txn.internal().id());
  lock_only_txn->mutable_internal()->set_type(TransactionType::LOCK_ONLY);
  lock_only_txn->mutable_internal()->mutable_involved_replicas()->CopyFrom(txn.internal().involved_replicas());

  return lock_only_txn;
}

}  // namespace slog