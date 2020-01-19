#include "module/sequencer.h"

#include "common/proto_utils.h"
#include "paxos/simple_multi_paxos.h"

namespace slog {

using internal::Batch;
using internal::Request;
using internal::Response;

Sequencer::Sequencer(
    shared_ptr<Configuration> config,
    Broker& broker)
  : BasicModule(
        broker.AddChannel(SEQUENCER_CHANNEL),
        config->GetBatchDuration()),
    config_(config),
    local_paxos_(new SimpleMultiPaxosClient(*this, LOCAL_PAXOS)),
    batch_(new Batch()),
    batch_id_counter_(0) {}

void Sequencer::HandleInternalRequest(
    Request&& req,
    string&& /* from_machine_id */) {
  switch (req.type_case()) {
    case Request::kForwardTxn: {
      auto forward_txn = req.mutable_forward_txn();
      PutTransactionIntoBatch(forward_txn->release_txn());
      break;
    }
    default:
      break;
  }
}

void Sequencer::HandleInternalResponse(
    Response&& res,
    string&& from_machine_id) {
  if (res.type_case() != Response::kForwardBatch  
      || res.forward_batch().batch_id() != current_batch_id_) {
    return;
  }

  auto machine_id = from_machine_id.empty() 
    ? config_->GetLocalMachineIdAsProto() 
    : MakeMachineIdProto(from_machine_id);

  if (machine_id.replica() != config_->GetLocalReplica()) {
    return;
  }
  pending_acks_.erase(machine_id.partition());
}

void Sequencer::HandlePeriodicWakeUp() {
  // TODO: Investigate whether ZMQ keeps messages in order. If it
  // does this wait for acks is unneccessary and should be removed
  // for better latency
 
  // Do nothing if we're still waiting for confirmation for the
  // latest batch or there is nothing to send
  if (!pending_acks_.empty() || batch_->transactions().empty()) {
    return;
  }

  auto batch_id = NextBatchId();

  VLOG(1) << "Finished batch " << batch_id 
          << ". Sending out for ordering and replicating";

  batch_->set_id(batch_id);

  Request req;
  auto forward_batch = req.mutable_forward_batch();
  forward_batch->set_allocated_batch(batch_.release());

  // Send batch id to local paxos for ordering
  local_paxos_->Propose(batch_id);

  // Replicate batch to all machines
  for (uint32_t part = 0; part < config_->GetNumPartitions(); part++) {
    for (uint32_t rep = 0; rep < config_->GetNumReplicas(); rep++) {
      Send(req, MakeMachineId(rep, part), SCHEDULER_CHANNEL);
    }
    pending_acks_.insert(part);
  }

  batch_.reset(new Batch());
}

void Sequencer::PutTransactionIntoBatch(Transaction* txn) {
  switch (txn->internal().type()) {
    case TransactionType::SINGLE_HOME:
      batch_->mutable_transactions()->AddAllocated(txn);
      break;
    case TransactionType::MULTI_HOME:
      break;
    case TransactionType::UNKNOWN:
      LOG(FATAL) << "Sequencer encountered a transaction with UNKNOWN type";
    default:
      break;
  }
}

BatchId Sequencer::NextBatchId() {
  batch_id_counter_++;
  current_batch_id_ =
      batch_id_counter_ * MAX_NUM_MACHINES + config_->GetLocalMachineIdAsNumber();
  return current_batch_id_;
}

} // namespace slog