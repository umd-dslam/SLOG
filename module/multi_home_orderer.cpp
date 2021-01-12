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
  NewBatch();
}

void MultiHomeOrderer::NewBatch() {
  if (batch_ == nullptr) {
    batch_.reset(new Batch());
  }
  batch_->Clear();
  batch_->set_transaction_type(TransactionType::MULTI_HOME);
  ++batch_id_counter_;
  batch_->set_id(batch_id_counter_ * kMaxNumMachines + config_->local_machine_id());
}

void MultiHomeOrderer::HandleInternalRequest(EnvelopePtr&& env) {
  auto request = env->mutable_request();
  switch (request->type_case()) {
    case Request::kForwardTxn: {
      // Received a new multi-home txn
      auto txn = request->mutable_forward_txn()->release_txn();

      TRACE(txn->mutable_internal(), TransactionEvent::ENTER_MULTI_HOME_ORDERER);

      ScheduleBatch(txn);
      break;
    }
    case Request::kForwardBatch:
      // Received a batch of multi-home txn replicated from another region
      ProcessForwardBatch(request->mutable_forward_batch());
      break;
    default:
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(request->type_case(), Request) << "\"";
      break;
  }
}

void MultiHomeOrderer::ProcessForwardBatch(internal::ForwardBatch* forward_batch) {
  switch (forward_batch->part_case()) {
    case internal::ForwardBatch::kBatchData: {
      auto batch = BatchPtr(forward_batch->release_batch_data());

      TRACE(batch.get(), TransactionEvent::ENTER_MULTI_HOME_ORDERER_IN_BATCH);

      multi_home_batch_log_.AddBatch(std::move(batch));
      break;
    }
    case internal::ForwardBatch::kBatchOrder: {
      auto& batch_order = forward_batch->batch_order();
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

void MultiHomeOrderer::ScheduleBatch(Transaction* txn) {
  DCHECK(txn->internal().type() == TransactionType::MULTI_HOME)
      << "Multi-home orderer batch can only contain multi-home txn. ";

  batch_->mutable_transactions()->AddAllocated(txn);
  // If this is the first txn of the batch, start the timer to send the batch
  if (batch_->transactions_size() == 1) {
    NewTimedCallback(batch_timeout_, [this]() {
      SendBatch();
      NewBatch();
    });
  }
}

void MultiHomeOrderer::SendBatch() {
  VLOG(3) << "Finished multi-home batch " << batch_->id() << " of size " << batch_->transactions().size();

  auto paxos_env = NewEnvelope();
  auto paxos_propose = paxos_env->mutable_request()->mutable_paxos_propose();
  paxos_propose->set_value(batch_->id());
  Send(move(paxos_env), kGlobalPaxos);

  // Replicate new batch to other regions
  auto batch_env = NewEnvelope();
  auto forward_batch = batch_env->mutable_request()->mutable_forward_batch();
  forward_batch->set_allocated_batch_data(batch_.release());

  auto part = config_->leader_partition_for_multi_home_ordering();
  auto num_replicas = config_->num_replicas();
  for (uint32_t rep = 0; rep < num_replicas; rep++) {
    auto machine_id = config_->MakeMachineId(rep, part);
    // If this is the same machine, we will send this batch later
    if (machine_id != config_->local_machine_id()) {
      Send(*batch_env, machine_id, kMultiHomeOrdererChannel);
    }
  }
  // If this to forward to the same machine, send the batch via pointer passing
  if (part == config_->local_partition()) {
    Send(move(batch_env), kMultiHomeOrdererChannel);
  }
}

}  // namespace slog