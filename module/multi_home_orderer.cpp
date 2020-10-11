#include "module/multi_home_orderer.h"

#include <glog/logging.h>

#include "common/constants.h"
#include "common/proto_utils.h"
#include "module/ticker.h"
#include "paxos/simple_multi_paxos.h"

namespace slog {

using internal::Batch;
using internal::Request;

MultiHomeOrderer::MultiHomeOrderer(
    const ConfigurationPtr& config,
    const shared_ptr<Broker>& broker) 
  : NetworkedModule(broker, kMultiHomeOrdererChannel),
    config_(config),
    batch_id_counter_(0) {
  NewBatch();
}

std::vector<zmq::socket_t> MultiHomeOrderer::InitializeCustomSockets() {
  vector<zmq::socket_t> ticker_socket;
  ticker_socket.push_back(Ticker::Subscribe(*GetContext()));
  return ticker_socket;
}

void MultiHomeOrderer::NewBatch() {
  batch_.reset(new Batch());
  batch_->set_transaction_type(TransactionType::MULTI_HOME);
}

void MultiHomeOrderer::HandleInternalRequest(Request&& req, MachineIdNum /* from */) {
  switch (req.type_case()) {
    case Request::kForwardTxn: {
      // Received a new multi-home txn
      auto txn = req.mutable_forward_txn()->release_txn();
      RecordTxnEvent(
          config_,
          txn->mutable_internal(),
          TransactionEvent::ENTER_MULTI_HOME_ORDERER);
      batch_->mutable_transactions()->AddAllocated(txn);
      break;
    }
    case Request::kForwardBatch:
      // Received a batch of multi-home txn replicated from another region
      ProcessForwardBatch(req.mutable_forward_batch());
      break;
    default:
      LOG(ERROR) << "Unexpected request type received: \""
                 << CASE_NAME(req.type_case(), Request) << "\"";
      break;
  }
}

void MultiHomeOrderer::HandleCustomSocket(zmq::socket_t& socket, size_t /* socket_index */) {
  // Remove the dummy message out of the queue
  zmq::message_t msg;
  (void)socket.recv(msg);

  if (batch_->transactions().empty()) {
    return;
  }

  auto batch_id = NextBatchId();
  batch_->set_id(batch_id);

  VLOG(1) << "Finished multi-home batch " << batch_id
          << ". Sending out for ordering and replicating";
  
  // Make a proposal for multi-home batch ordering
  Request paxos_req;
  auto paxos_propose = paxos_req.mutable_paxos_propose();
  paxos_propose->set_value(batch_id);
  Send(paxos_req, kGlobalPaxos);

  Request batch_req;
  auto forward_batch = batch_req.mutable_forward_batch();
  forward_batch->set_allocated_batch_data(batch_.release());

  // Replicate new batch to other regions
  auto part = config_->GetLeaderPartitionForMultiHomeOrdering();
  auto num_replicas = config_->GetNumReplicas();
  for (uint32_t rep = 0; rep < num_replicas; rep++) {
    auto machine_id = config_->MakeMachineIdNum(rep, part);
    Send(batch_req, kMultiHomeOrdererChannel, machine_id);
  }

  NewBatch();
}

void MultiHomeOrderer::ProcessForwardBatch(
    internal::ForwardBatch* forward_batch) {
  switch (forward_batch->part_case()) {
    case internal::ForwardBatch::kBatchData: {
      auto batch = BatchPtr(forward_batch->release_batch_data());
      RecordTxnEvent(
          config_,
          batch.get(),
          TransactionEvent::ENTER_MULTI_HOME_ORDERER_IN_BATCH);
      multi_home_batch_log_.AddBatch(std::move(batch));
      break;
    }
    case internal::ForwardBatch::kBatchOrder: {
      auto& batch_order = forward_batch->batch_order();
      multi_home_batch_log_.AddSlot(
          batch_order.slot(), batch_order.batch_id());
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

    Request req;
    auto forward_batch = req.mutable_forward_batch();
    forward_batch->set_allocated_batch_data(batch.release());

    RecordTxnEvent(
        config_,
        forward_batch->mutable_batch_data(),
        TransactionEvent::EXIT_MULTI_HOME_ORDERER_IN_BATCH);

    // Send the newly ordered multi-home batch to the sequencer
    Send(req, kSequencerChannel);
  }
}

BatchId MultiHomeOrderer::NextBatchId() {
  batch_id_counter_++;
  return batch_id_counter_ * kMaxNumMachines + config_->GetLocalMachineIdAsNumber();
}

} // namespace slog