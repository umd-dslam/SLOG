#include "module/multi_home_orderer.h"

#include <glog/logging.h>

#include "common/constants.h"
#include "common/json_utils.h"
#include "common/proto_utils.h"
#include "paxos/simulated_multi_paxos.h"

using std::shared_ptr;

namespace slog {

using internal::Batch;
using internal::Envelope;
using internal::Request;

MultiHomeOrderer::MultiHomeOrderer(const shared_ptr<Broker>& broker, const MetricsRepositoryManagerPtr& metrics_manager,
                                   std::chrono::milliseconds poll_timeout)
    : NetworkedModule(broker, kMultiHomeOrdererChannel, metrics_manager, poll_timeout),
      batch_id_counter_(0),
      collecting_stats_(false) {
  batch_per_rep_.resize(config()->num_replicas());
  NewBatch();
}

void MultiHomeOrderer::NewBatch() {
  ++batch_id_counter_;
  batch_size_ = 0;
  for (auto& batch : batch_per_rep_) {
    if (batch == nullptr) {
      batch.reset(new Batch());
    }
    batch->Clear();
    batch->set_transaction_type(TransactionType::MULTI_HOME_OR_LOCK_ONLY);
    batch->set_id(batch_id());
  }
}

void MultiHomeOrderer::OnInternalRequestReceived(EnvelopePtr&& env) {
  auto request = env->mutable_request();
  switch (request->type_case()) {
    case Request::kForwardTxn: {
      // Received a new multi-home txn
      auto txn = request->mutable_forward_txn()->release_txn();

      RECORD(txn->mutable_internal(), TransactionEvent::ENTER_MULTI_HOME_ORDERER);

      AddToBatch(txn);
      break;
    }
    case Request::kForwardBatchData:
      // Received a batch of multi-home txn replicated from another region
      ProcessForwardBatchData(move(env));
      break;
    case Request::kForwardBatchOrder:
      ProcessForwardBatchOrder(move(env));
      break;
    case Request::kStats:
      ProcessStatsRequest(env->request().stats());
      break;
    default:
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(request->type_case(), Request) << "\"";
      break;
  }
}

void MultiHomeOrderer::ProcessForwardBatchData(EnvelopePtr&& env) {
  auto batch = BatchPtr(env->mutable_request()->mutable_forward_batch_data()->mutable_batch_data()->ReleaseLast());

  RECORD(batch.get(), TransactionEvent::ENTER_MULTI_HOME_ORDERER_IN_BATCH);

  VLOG(1) << "Received data for MULTI-HOME batch " << batch->id() << " from [" << env->from()
          << "]. Number of txns: " << batch->transactions_size();

  multi_home_batch_log_.AddBatch(std::move(batch));

  AdvanceLog();
}

void MultiHomeOrderer::ProcessForwardBatchOrder(EnvelopePtr&& env) {
  auto& batch_order = env->request().forward_batch_order().remote_batch_order();

  VLOG(1) << "Received order for batch " << batch_order.batch_id() << " from [" << env->from()
          << "]. Slot: " << batch_order.slot();

  multi_home_batch_log_.AddSlot(batch_order.slot(), batch_order.batch_id());

  AdvanceLog();
}

void MultiHomeOrderer::AdvanceLog() {
  while (multi_home_batch_log_.HasNextBatch()) {
    auto batch_and_slot = multi_home_batch_log_.NextBatch();
    auto& batch = batch_and_slot.second;

    VLOG(1) << "Processing batch " << batch->id();

    auto transactions = Unbatch(batch.get());
    for (auto txn : transactions) {
      RECORD(txn->mutable_internal(), TransactionEvent::EXIT_MULTI_HOME_ORDERER);

      auto env = NewEnvelope();
      auto forward_txn = env->mutable_request()->mutable_forward_txn();
      forward_txn->set_allocated_txn(txn);
      Send(move(env), kSequencerChannel);
    }
  }
}

void MultiHomeOrderer::AddToBatch(Transaction* txn) {
  DCHECK(txn->internal().type() == TransactionType::MULTI_HOME_OR_LOCK_ONLY)
      << "Multi-home orderer batch can only contain multi-home txn. ";

  auto& replicas = txn->internal().involved_replicas();
  for (int i = 0; i < replicas.size() - 1; i++) {
    batch_per_rep_[replicas[i]]->add_transactions()->CopyFrom(*txn);
  }
  // Add the last one directly instead of copying
  batch_per_rep_[replicas[replicas.size() - 1]]->mutable_transactions()->AddAllocated(txn);

  ++batch_size_;

  // If this is the first txn in the batch, schedule to send the batch at a later time
  if (batch_size_ == 1) {
    NewTimedCallback(config()->sequencer_batch_duration(), [this]() {
      SendBatch();
      NewBatch();
    });

    batch_starting_time_ = std::chrono::steady_clock::now();
  }
}

void MultiHomeOrderer::SendBatch() {
  VLOG(3) << "Finished multi-home batch " << batch_id() << " of size " << batch_size_;

  if (collecting_stats_) {
    stat_batch_sizes_.push_back(batch_size_);
    stat_batch_durations_ms_.push_back((std::chrono::steady_clock::now() - batch_starting_time_).count() / 1000000.0);
  }

  auto paxos_env = NewEnvelope();
  auto paxos_propose = paxos_env->mutable_request()->mutable_paxos_propose();
  paxos_propose->set_value(batch_id());
  Send(move(paxos_env), config()->MakeMachineId(config()->leader_replica_for_multi_home_ordering(), 0), kGlobalPaxos);

  // Replicate new batch to other regions
  auto part = config()->leader_partition_for_multi_home_ordering();
  for (uint32_t rep = 0; rep < config()->num_replicas(); rep++) {
    auto env = NewEnvelope();
    auto forward_batch = env->mutable_request()->mutable_forward_batch_data();
    forward_batch->mutable_batch_data()->AddAllocated(batch_per_rep_[rep].release());
    Send(move(env), config()->MakeMachineId(rep, part), kMultiHomeOrdererChannel);
  }
}

/**
 * {
 *    mho_batch_size_pctls:        [int],
 *    mho_batch_duration_ms_pctls: [float]
 * }
 */
void MultiHomeOrderer::ProcessStatsRequest(const internal::StatsRequest& stats_request) {
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

  stats.AddMember(StringRef(MHO_BATCH_SIZE_PCTLS), Percentiles(stat_batch_sizes_, alloc), alloc);
  stat_batch_sizes_.clear();

  stats.AddMember(StringRef(MHO_BATCH_DURATION_MS_PCTLS), Percentiles(stat_batch_durations_ms_, alloc), alloc);
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