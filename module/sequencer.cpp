#include "module/sequencer.h"

#include <glog/logging.h>

#include <algorithm>

#include "common/monitor.h"
#include "common/proto_utils.h"
#include "module/ticker.h"
#include "paxos/simulated_multi_paxos.h"

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
  if (env->request().type_case() != Request::kForwardTxn) {
    return;
  }
  auto txn = env->mutable_request()->mutable_forward_txn()->release_txn();

  TRACE(txn->mutable_internal(), TransactionEvent::ENTER_SEQUENCER);

  if (txn->internal().type() == TransactionType::MULTI_HOME_OR_LOCK_ONLY) {
    txn = GenerateLockOnlyTxn(txn, config_->local_replica(), true /* in_place */);
  }

  for (auto p : txn->internal().involved_partitions()) {
    auto new_txn = GeneratePartitionedTxn(config_, *txn, p);
    CHECK(new_txn != nullptr);

    // Check if the generated subtxn does not intend to lock any key in its home region
    // If this is a remaster txn, it is never redundant
    auto is_redundant = !new_txn->has_remaster();
    for (const auto& kv : new_txn->keys()) {
      if (static_cast<int>(kv.second.metadata().master()) == new_txn->internal().home()) {
        is_redundant = false;
        break;
      }
    }
    if (!is_redundant) {
      partitioned_batch_[p]->mutable_transactions()->AddAllocated(new_txn);
    }
  }
  delete txn;

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

  VLOG(3) << "Delay batch " << batch_id() << " for " << delay_ms << " ms";

  for (uint32_t part = 0; part < config_->num_partitions(); part++) {
    auto env = NewBatchRequest(partitioned_batch_[part].release());

    // Send to the partition in the local replica immediately
    Send(*env, config_->MakeMachineId(config_->local_replica(), part), kInterleaverChannel);

    NewTimedCallback(milliseconds(delay_ms), [this, part, delayed_env = env.release()]() {
      VLOG(3) << "Sending delayed batch " << delayed_env->request().forward_batch().batch_data().id();
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

}  // namespace slog