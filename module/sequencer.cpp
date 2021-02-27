#include "module/sequencer.h"

#include <glog/logging.h>

#include <algorithm>

#include "common/json_utils.h"
#include "common/monitor.h"
#include "common/proto_utils.h"
#include "module/ticker.h"
#include "paxos/simulated_multi_paxos.h"

using std::move;

namespace slog {

using internal::Batch;
using internal::Request;
using internal::Response;

Sequencer::Sequencer(const ConfigurationPtr& config, const std::shared_ptr<Broker>& broker, milliseconds poll_timeout)
    : NetworkedModule("Sequencer", broker, kSequencerChannel, poll_timeout),
      config_(config),
      batch_id_counter_(0),
      collecting_stats_(false) {
  partitioned_batch_.resize(config_->num_partitions());
  NewBatch();
}

void Sequencer::NewBatch() {
  ++batch_id_counter_;
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

void Sequencer::OnInternalRequestReceived(EnvelopePtr&& env) {
  switch (env->request().type_case()) {
    case Request::kForwardTxn:
      ProcessForwardTxn(move(env));
      break;
    case Request::kStats:
      ProcessStatsRequest(env->request().stats());
      break;
    default:
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(env->request().type_case(), Request) << "\"";
      break;
  }
}

void Sequencer::ProcessForwardTxn(EnvelopePtr&& env) {
  auto txn = env->mutable_request()->mutable_forward_txn()->release_txn();

  TRACE(txn->mutable_internal(), TransactionEvent::ENTER_SEQUENCER);

  if (txn->internal().type() == TransactionType::MULTI_HOME_OR_LOCK_ONLY) {
    txn = GenerateLockOnlyTxn(txn, config_->local_replica(), true /* in_place */);
  }

  auto num_involved_partitions = txn->internal().involved_partitions_size();
  for (int i = 0; i < num_involved_partitions; ++i) {
    bool in_place = i == (num_involved_partitions - 1);
    auto p = txn->internal().involved_partitions(i);
    auto new_txn = GeneratePartitionedTxn(config_, txn, p, in_place);
    if (new_txn != nullptr) {
      partitioned_batch_[p]->mutable_transactions()->AddAllocated(new_txn);
    }
  }

  ++batch_size_;

  // If this is the first txn in the batch, schedule to send the batch at a later time
  if (batch_size_ == 1) {
    NewTimedCallback(config_->sequencer_batch_duration(), [this]() {
      SendBatch();
      NewBatch();
    });

    batch_starting_time_ = steady_clock::now();
  }

  // Batch size is larger than the maximum size, send the batch immediately
  auto max_batch_size = config_->sequencer_max_batch_size();
  if (max_batch_size > 0 && batch_size_ >= max_batch_size) {
    ClearTimedCallbacks();
    SendBatch();
    NewBatch();
  }
}

void Sequencer::SendBatch() {
  VLOG(3) << "Finished batch " << batch_id() << " of size " << batch_size_
          << ". Sending out for ordering and replicating";

  if (collecting_stats_) {
    stat_batch_sizes_.push_back(batch_size_);
    stat_batch_durations_ms_.push_back((steady_clock::now() - batch_starting_time_).count() / 1000000.0);
  }

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

/**
 * {
 *    seq_batch_size_pctls:        [int],
 *    seq_batch_duration_ms_pctls: [float]
 * }
 */
void Sequencer::ProcessStatsRequest(const internal::StatsRequest& stats_request) {
  using rapidjson::StringRef;

  int level = stats_request.level();

  rapidjson::Document stats;
  stats.SetObject();
  auto& alloc = stats.GetAllocator();

  if (level == 0) {
    collecting_stats_ = false;
  } else if (level > 0) {
    collecting_stats_ = true;
  }

  stats.AddMember(StringRef(SEQ_BATCH_SIZE_PCTLS), Percentiles(stat_batch_sizes_, alloc), alloc);
  stat_batch_sizes_.clear();

  stats.AddMember(StringRef(SEQ_BATCH_DURATION_MS_PCTLS), Percentiles(stat_batch_durations_ms_, alloc), alloc);
  stat_batch_durations_ms_.clear();

  // Write JSON object to a buffer and send back to the server
  rapidjson::StringBuffer buf;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
  stats.Accept(writer);

  auto env = NewEnvelope();
  env->mutable_response()->mutable_stats()->set_id(stats_request.id());
  env->mutable_response()->mutable_stats()->set_stats_json(buf.GetString());
  Send(move(env), kServerChannel);
}

}  // namespace slog