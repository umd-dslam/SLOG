#include "module/sequencer.h"

#include <glog/logging.h>

#include <algorithm>

#include "common/json_utils.h"
#include "common/proto_utils.h"
#include "paxos/simulated_multi_paxos.h"

using std::move;

namespace slog {

using internal::Batch;
using internal::Request;
using internal::Response;

using std::chrono::milliseconds;

Sequencer::Sequencer(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
                     const MetricsRepositoryManagerPtr& metrics_manager, milliseconds poll_timeout)
    : NetworkedModule(context, config, config->sequencer_port(), kSequencerChannel, metrics_manager, poll_timeout),
      sharder_(Sharder::MakeSharder(config)),
      batch_id_counter_(0),
      rg_(std::random_device()()),
      collecting_stats_(false) {
  StartOver();
}

void Sequencer::StartOver() {
  total_batch_size_ = 0;
  batches_.clear();
  NewBatch();
}

void Sequencer::NewBatch() {
  current_batch_size_ = 0;
  ++batch_id_counter_;

  PartitionedBatch new_batch(config()->num_partitions());
  for (auto& partition : new_batch) {
    partition.reset(new Batch());
    partition->set_transaction_type(TransactionType::SINGLE_HOME);
    partition->set_id(batch_id());
  }

  batches_.push_back(std::move(new_batch));
}

void Sequencer::OnInternalRequestReceived(EnvelopePtr&& env) {
  auto request = env->mutable_request();
  switch (request->type_case()) {
    case Request::kForwardTxn: {
      auto txn = request->mutable_forward_txn()->release_txn();
      if (txn->internal().sequencer_delay_ms() > 0) {
        auto delay = milliseconds(txn->internal().sequencer_delay_ms());
        NewTimedCallback(delay, [this, txn]() { BatchTxn(txn); });
      } else {
        BatchTxn(txn);
      }
      break;
    }
    case Request::kStats:
      ProcessStatsRequest(request->stats());
      break;
    default:
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(request->type_case(), Request) << "\"";
      break;
  }
}

void Sequencer::BatchTxn(Transaction* txn) {
  RECORD(txn->mutable_internal(), TransactionEvent::ENTER_SEQUENCER);

  if (txn->internal().type() == TransactionType::MULTI_HOME_OR_LOCK_ONLY) {
    txn = GenerateLockOnlyTxn(txn, config()->local_replica(), true /* in_place */);
  }

  auto& current_batch = batches_.back();
  auto num_involved_partitions = txn->internal().involved_partitions_size();
  for (int i = 0; i < num_involved_partitions; ++i) {
    bool in_place = i == (num_involved_partitions - 1);
    auto p = txn->internal().involved_partitions(i);
    auto new_txn = GeneratePartitionedTxn(sharder_, txn, p, in_place);
    if (new_txn != nullptr) {
      current_batch[p]->mutable_transactions()->AddAllocated(new_txn);
    }
  }

  ++current_batch_size_;
  ++total_batch_size_;

  // If this is the first txn after starting over, schedule to send the batch at a later time
  if (total_batch_size_ == 1) {
    NewTimedCallback(config()->sequencer_batch_duration(), [this]() {
      SendBatches();
      StartOver();
    });

    batch_starting_time_ = std::chrono::steady_clock::now();
  }

  auto max_batch_size = config()->sequencer_batch_size();
  if (max_batch_size > 0 && current_batch_size_ >= max_batch_size) {
    NewBatch();
  }
}

void Sequencer::SendBatches() {
  VLOG(3) << "Finished up to batch " << batch_id() << " with " << total_batch_size_ << " txns to be replicated. "
          << "Sending out for ordering and replicating";

  if (collecting_stats_) {
    stat_batch_sizes_.push_back(total_batch_size_);
    stat_batch_durations_ms_.push_back((std::chrono::steady_clock::now() - batch_starting_time_).count() / 1000000.0);
  }

  auto local_replica = config()->local_replica();
  auto local_partition = config()->local_partition();
  auto num_replicas = config()->num_replicas();
  auto num_partitions = config()->num_partitions();

  int home_position = batch_id_counter_ - batches_.size();
  for (auto& batch : batches_) {
    auto batch_id = batch[0]->id();

    // Propose a new batch
    auto paxos_env = NewEnvelope();
    auto paxos_propose = paxos_env->mutable_request()->mutable_paxos_propose();
    paxos_propose->set_value(local_partition);
    Send(move(paxos_env), kLocalPaxos);

    // Distribute the batch data to other partitions in the same replica
    vector<internal::Batch*> batch_partitions;
    for (uint32_t p = 0; p < num_partitions; p++) {
      auto batch_partition = batch[p].release();

      RECORD(batch_partition, TransactionEvent::EXIT_SEQUENCER_IN_BATCH);

      auto env = NewBatchForwardingMessage({batch_partition}, home_position);
      Send(*env, config()->MakeMachineId(local_replica, p), kLocalLogChannel);
      // Collect back the batch partition to send to other replicas
      batch_partitions.push_back(
          env->mutable_request()->mutable_forward_batch_data()->mutable_batch_data()->ReleaseLast());
    }

    // Distribute the batch data to other replicas. All partitions of current batch are contained in a single message
    auto env = NewBatchForwardingMessage(move(batch_partitions), home_position);
    vector<MachineId> destinations;
    destinations.reserve(num_replicas);
    for (uint32_t rep = 0; rep < num_replicas; rep++) {
      if (rep != local_replica) {
        // Send to a fixed partition of the destination replica to avoid reordering.
        // The partition is selected such that the logs are evenly distributed over
        // all partitions
        auto part = (rep + num_replicas - local_replica) % num_replicas % num_partitions;
        destinations.push_back(config()->MakeMachineId(rep, part));
      }
    }

    // Deliberately delay the batch as specified in the config
    if (config()->replication_delay_pct()) {
      std::bernoulli_distribution is_delayed(config()->replication_delay_pct() / 100.0);
      if (is_delayed(rg_)) {
        auto delay_ms = config()->replication_delay_amount_ms();

        VLOG(3) << "Delay batch " << batch_id << " for " << delay_ms << " ms";

        NewTimedCallback(milliseconds(delay_ms),
                        [this, destinations, batch_id, delayed_env = env.release()]() {
                          VLOG(3) << "Sending delayed batch " << batch_id;
                          Send(*delayed_env, destinations, kInterleaverChannel);
                          delete delayed_env;
                        });

        return;
      }
    }

    Send(*env, destinations, kInterleaverChannel);

    home_position++;
  }
}

EnvelopePtr Sequencer::NewBatchForwardingMessage(std::vector<internal::Batch*>&& batch, int home_position) {
  auto env = NewEnvelope();
  auto forward_batch = env->mutable_request()->mutable_forward_batch_data();
  forward_batch->set_home(config()->local_replica());
  forward_batch->set_home_position(home_position);
  for (auto b : batch) {
    forward_batch->mutable_batch_data()->AddAllocated(b);
  }
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