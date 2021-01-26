#include "module/multi_home_orderer.h"

#include <glog/logging.h>

#include "common/constants.h"
#include "common/monitor.h"
#include "common/proto_utils.h"
#include "module/ticker.h"
#include "paxos/simple_multi_paxos.h"

namespace slog {

using internal::Batch;
using internal::Envelope;
using internal::Request;

MultiHomeOrderer::MultiHomeOrderer(const ConfigurationPtr& config, const shared_ptr<Broker>& broker,
                                   milliseconds batch_timeout, std::chrono::milliseconds poll_timeout)
    : NetworkedModule("MultiHomeOrderer", broker, kMultiHomeOrdererChannel, poll_timeout),
      config_(config),
      batch_timeout_(batch_timeout),
      batch_id_counter_(0) {
  batch_per_rep_.resize(config_->num_replicas());
  NewBatch();
}

void MultiHomeOrderer::NewBatch() {
  ++batch_id_counter_;
  batch_scheduled_ = false;
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

void MultiHomeOrderer::HandleInternalRequest(EnvelopePtr&& env) {
  auto request = env->mutable_request();
  switch (request->type_case()) {
    case Request::kForwardTxn: {
      // Received a new multi-home txn
      auto txn = request->mutable_forward_txn()->release_txn();

      TRACE(txn->mutable_internal(), TransactionEvent::ENTER_MULTI_HOME_ORDERER);

      AddToBatch(txn);
      break;
    }
    case Request::kForwardBatch:
      // Received a batch of multi-home txn replicated from another region
      ProcessForwardBatch(move(env));
      break;
    default:
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(request->type_case(), Request) << "\"";
      break;
  }
}

void MultiHomeOrderer::ProcessForwardBatch(EnvelopePtr&& env) {
  auto forward_batch = env->mutable_request()->mutable_forward_batch();
  switch (forward_batch->part_case()) {
    case internal::ForwardBatch::kBatchData: {
      auto batch = BatchPtr(forward_batch->release_batch_data());

      TRACE(batch.get(), TransactionEvent::ENTER_MULTI_HOME_ORDERER_IN_BATCH);

      VLOG(1) << "Received data for MULTI-HOME batch " << batch->id() << " from [" << env->from()
              << "]. Number of txns: " << batch->transactions_size();

      multi_home_batch_log_.AddBatch(std::move(batch));
      break;
    }
    case internal::ForwardBatch::kBatchOrder: {
      auto& batch_order = forward_batch->batch_order();

      VLOG(1) << "Received order for batch " << batch_order.batch_id() << " from [" << env->from()
              << "]. Slot: " << batch_order.slot();

      multi_home_batch_log_.AddSlot(batch_order.slot(), batch_order.batch_id());
      break;
    }
    default:
      break;
  }

  while (multi_home_batch_log_.HasNextBatch()) {
    auto batch_and_slot = multi_home_batch_log_.NextBatch();
    auto slot = batch_and_slot.first;
    auto& batch = batch_and_slot.second;

    VLOG(1) << "Processing batch " << batch->id();

    // Replace the batch id with its slot number so that it is
    // easier to determine the batch order later on
    batch->set_id(slot);

    auto env = NewEnvelope();
    auto forward_batch = env->mutable_request()->mutable_forward_batch();
    forward_batch->set_allocated_batch_data(batch.release());

    TRACE(forward_batch->mutable_batch_data(), TransactionEvent::EXIT_MULTI_HOME_ORDERER_IN_BATCH);

    // Send the newly ordered multi-home batch to the sequencer
    Send(move(env), kSequencerChannel);
  }
}

void MultiHomeOrderer::AddToBatch(Transaction* txn) {
  DCHECK(txn->internal().type() == TransactionType::MULTI_HOME_OR_LOCK_ONLY)
      << "Multi-home orderer batch can only contain multi-home txn. ";

  auto& replicas = txn->internal().involved_replicas();
  for (int i = 0; i < replicas.size() - 1; i++) {
    batch_per_rep_[replicas[i]]->add_transactions()->CopyFrom(*txn);
  }
  // Add the last one directly to avoid copying
  batch_per_rep_[replicas[replicas.size() - 1]]->mutable_transactions()->AddAllocated(txn);

  ++batch_size_;

  // If this is the first txn of the batch, start the timer to send the batch
  if (!batch_scheduled_) {
    NewTimedCallback(batch_timeout_, [this]() {
      SendBatch();
      NewBatch();
    });
    batch_scheduled_ = true;
  }
}

void MultiHomeOrderer::SendBatch() {
  VLOG(3) << "Finished multi-home batch " << batch_id() << " of size " << batch_size_;

  auto paxos_env = NewEnvelope();
  auto paxos_propose = paxos_env->mutable_request()->mutable_paxos_propose();
  paxos_propose->set_value(batch_id());
  Send(move(paxos_env), config_->MakeMachineId(config_->leader_replica_for_multi_home_ordering(), 0), kGlobalPaxos);

  // Replicate new batch to other regions
  auto part = config_->leader_partition_for_multi_home_ordering();
  for (uint32_t rep = 0; rep < config_->num_replicas(); rep++) {
    auto env = NewEnvelope();
    auto forward_batch = env->mutable_request()->mutable_forward_batch();
    forward_batch->set_allocated_batch_data(batch_per_rep_[rep].release());
    Send(move(env), config_->MakeMachineId(rep, part), kMultiHomeOrdererChannel);
  }
}

}  // namespace slog