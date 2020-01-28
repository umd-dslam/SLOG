#include "module/multi_home_orderer.h"

#include "common/constants.h"
#include "common/proto_utils.h"
#include "paxos/simple_multi_paxos.h"

namespace slog {

using internal::Batch;
using internal::Request;

MultiHomeOrderer::MultiHomeOrderer(
    shared_ptr<Configuration> config,
    Broker& broker) 
  : BasicModule(
        broker.AddChannel(MULTI_HOME_ORDERER_CHANNEL),
        config->GetBatchDuration()),
    config_(config),
    global_paxos_(new SimpleMultiPaxosClient(*this, GLOBAL_PAXOS)),
    batch_(new Batch()),
    batch_id_counter_(0) {}

void MultiHomeOrderer::HandleInternalRequest(
    Request&& req,
    string&& /* from_machine_id */) {
  switch (req.type_case()) {
    case Request::kForwardTxn: {
      // Received a new multi-home txn
      auto txn = req.mutable_forward_txn()->release_txn();
      batch_->mutable_transactions()->AddAllocated(txn);
      break;
    }
    case Request::kForwardBatch:
      // Received a batch of multi-home txn replicated from another region
      ProcessForwardBatchRequest(req.mutable_forward_batch());
      break;
    default:
      LOG(ERROR) << "Unexpected request type received: \""
                 << CASE_NAME(req.type_case(), Request) << "\"";
      break;
  }
}

void MultiHomeOrderer::HandlePeriodicWakeUp() {
  auto batch_id = NextBatchId();
  batch_->set_id(batch_id);

  VLOG(1) << "Finished multi-home batch " << batch_id
          << "Sending out for ordering and replicating";
  
  Request req;
  auto forward_batch = req.mutable_forward_batch();
  forward_batch->set_allocated_batch_data(batch_.release());

  // Make a proposal for multi-home batch ordering
  global_paxos_->Propose(batch_id);

  // Replicate new batch to other regions
  auto part = config_->GetLeaderPartitionForMultiHomeOrdering();
  for (uint32_t rep = 0; rep < config_->GetNumReplicas(); rep++) {
    Send(req, MakeMachineId(rep, part), MULTI_HOME_ORDERER_CHANNEL);
  }

  batch_.reset(new Batch());
}

void MultiHomeOrderer::ProcessForwardBatchRequest(
    internal::ForwardBatchRequest* forward_batch) {
  switch (forward_batch->part_case()) {
    case internal::ForwardBatchRequest::kBatchData: {
      auto batch = BatchPtr(forward_batch->release_batch_data());
      multi_home_batch_log_.AddBatch(std::move(batch));
      break;
    }
    case internal::ForwardBatchRequest::kBatchOrder: {
      auto& batch_order = forward_batch->batch_order();
      multi_home_batch_log_.AddSlot(
          batch_order.slot(), batch_order.batch_id());
      break;
    }
    default:
      break;
  }

  while (multi_home_batch_log_.HasNextBatch()) {
    auto batch = multi_home_batch_log_.NextBatch();
    Request req;
    auto forward_batch = req.mutable_forward_batch();
    forward_batch->set_allocated_batch_data(batch.release());
    // Send the newly ordered multi-home batch to the sequencer
    SendSameMachine(req, SEQUENCER_CHANNEL);
  }
}

BatchId MultiHomeOrderer::NextBatchId() {
  batch_id_counter_++;
  return batch_id_counter_ * MAX_NUM_MACHINES + config_->GetLocalMachineIdAsNumber();
}

} // namespace slog