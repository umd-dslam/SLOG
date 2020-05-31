#include "module/sequencer.h"

#include "common/proto_utils.h"
#include "module/ticker.h"
#include "paxos/simple_multi_paxos.h"

namespace slog {

using internal::Batch;
using internal::Request;
using internal::Response;

Sequencer::Sequencer(const ConfigurationPtr& config, Broker& broker)
  : BasicModule(
        "Sequencer",
        broker.AddChannel(SEQUENCER_CHANNEL)),
    config_(config),
    local_paxos_(new SimpleMultiPaxosClient(*this, LOCAL_PAXOS)),
    batch_id_counter_(0) {
  NewBatch();
}

vector<zmq::socket_t> Sequencer::InitializeCustomSockets() {
  vector<zmq::socket_t> ticker_socket;
  ticker_socket.push_back(Ticker::Subscribe(*GetContext()));
  return ticker_socket;
}

void Sequencer::NewBatch() {
  batch_.reset(new Batch());
  batch_->set_transaction_type(TransactionType::SINGLE_HOME);
}

void Sequencer::HandleInternalRequest(
    Request&& req,
    string&& /* from_machine_id */) {
  switch (req.type_case()) {
    case Request::kForwardTxn: {
      // Received a single-home txn
      auto txn = req.mutable_forward_txn()->release_txn();

      RecordTxnEvent(
          config_,
          txn->mutable_internal(),
          TransactionEvent::ENTER_SEQUENCER);

      PutSingleHomeTransactionIntoBatch(txn);
      break;
    }
    case Request::kForwardBatch: {
      // Received a batch of multi-home txns
      if (req.forward_batch().part_case() == internal::ForwardBatch::kBatchData) {
        ProcessMultiHomeBatch(std::move(req));
      }
      break;
    }
    default:
      LOG(ERROR) << "Unexpected request type received: \""
                 << CASE_NAME(req.type_case(), Request) << "\"";
      break;
  }
}

void Sequencer::HandleCustomSocketMessage(
    const MMessage& /* msg */,
    size_t /* socket_index */) {

  // Do nothing if there is nothing to send
  if (batch_->transactions().empty()) {
    return;
  }

  auto batch_id = NextBatchId();
  batch_->set_id(batch_id);

  VLOG(3) << "Finished batch " << batch_id
          << ". Sending out for ordering and replicating";

  Request req;
  auto forward_batch = req.mutable_forward_batch();
  // minus 1 so that batch id counter starts from 0
  forward_batch->set_same_origin_position(batch_id_counter_ - 1);
  forward_batch->set_allocated_batch_data(batch_.release());

  // Send batch id to local paxos for ordering
  local_paxos_->Propose(config_->GetLocalPartition());

  RecordTxnEvent(
      config_,
      forward_batch->mutable_batch_data(),
      TransactionEvent::EXIT_SEQUENCER_IN_BATCH);

#ifdef ENABLE_REPLICATION_DELAY
  MaybeSendDelayedBatches();

  // Maybe delay current batch
  if (rand() % 100 < config_->GetReplicationDelayPercent()) {
    DelaySingleHomeBatch(std::move(req));
    NewBatch();
    return;
  } // Else send it normally
#endif /* GetReplicationDelayEnabled */

  // Replicate batch to all machines
  auto num_partitions = config_->GetNumPartitions();
  auto num_replicas = config_->GetNumReplicas();
  for (uint32_t part = 0; part < num_partitions; part++) {
    for (uint32_t rep = 0; rep < num_replicas; rep++) {
      auto machine_id = MakeMachineIdAsString(rep, part);
      Send(
          req,
          machine_id,
          SCHEDULER_CHANNEL,
          part + 1 < num_partitions || rep + 1 < num_replicas /* has_more */);
    }
  }

  NewBatch();
}

void Sequencer::ProcessMultiHomeBatch(Request&& req) {
  auto batch = req.mutable_forward_batch()->mutable_batch_data();
  if (batch->transaction_type() != TransactionType::MULTI_HOME) {
    LOG(ERROR) << "Batch has to contain multi-home txns";
    return;
  }

  RecordTxnEvent(
      config_,
      batch,
      TransactionEvent::ENTER_SEQUENCER_IN_BATCH);

  auto local_rep = config_->GetLocalReplica();
  // For each multi-home txn, create a lock-only txn and put into
  // the single-home batch to be sent to the local log
  for (auto& txn : batch->transactions()) {
    auto lock_only_txn = new Transaction();

    lock_only_txn->mutable_internal()
        ->set_id(txn.internal().id());

    lock_only_txn->mutable_internal()
        ->set_type(TransactionType::LOCK_ONLY);
    
    const auto& metadata = txn.internal().master_metadata();
    auto lock_only_metadata = lock_only_txn->mutable_internal()->mutable_master_metadata();
    
    for (auto& key_value : txn.read_set()) {
      auto master = metadata.at(key_value.first).master();
      if (master == local_rep) {
        lock_only_txn->mutable_read_set()->insert(key_value);
        lock_only_metadata->insert({key_value.first, metadata.at(key_value.first)});
      }
    }
    for (auto& key_value : txn.write_set()) {
      auto master = metadata.at(key_value.first).master();
      if (master == local_rep) {
        lock_only_txn->mutable_write_set()->insert(key_value);
        lock_only_metadata->insert({key_value.first, metadata.at(key_value.first)});
      }
    }
    if (!lock_only_txn->read_set().empty() || !lock_only_txn->write_set().empty()) {
      PutSingleHomeTransactionIntoBatch(lock_only_txn);
    }
  }

  // Replicate the batch of multi-home txns to all machines in the same region
  RecordTxnEvent(
      config_,
      batch,
      TransactionEvent::EXIT_SEQUENCER_IN_BATCH);

  auto num_partitions = config_->GetNumPartitions();
  for (uint32_t part = 0; part < num_partitions; part++) {
    auto machine_id = MakeMachineIdAsString(local_rep, part);
    Send(
        req,
        machine_id,
        SCHEDULER_CHANNEL,
        part + 1 < num_partitions /* has_more */);
  }
}

void Sequencer::PutSingleHomeTransactionIntoBatch(Transaction* txn) {
  CHECK(
      txn->internal().type() == TransactionType::SINGLE_HOME
      || txn->internal().type() == TransactionType::LOCK_ONLY)
      << "Sequencer batch can only contain single-home or lock-only txn. "
      << "Multi-home txn or unknown txn type received instead.";
  batch_->mutable_transactions()->AddAllocated(txn);
}

BatchId Sequencer::NextBatchId() {
  batch_id_counter_++;
  return batch_id_counter_ * MAX_NUM_MACHINES + config_->GetLocalMachineIdAsNumber();
}

#ifdef ENABLE_REPLICATION_DELAY
void Sequencer::DelaySingleHomeBatch(internal::Request&& request) {
  delayed_batches_.push_back(request);

  // Send the batch to the local scheduler only
  auto rep = config_->GetLocalReplica();
  auto part = config_->GetLocalPartition();
  auto machine_id = MakeMachineIdAsString(rep, part);
  Send(
      request,
      machine_id,
      SCHEDULER_CHANNEL,
      // Note: atomicity is lost already, since the batch
      // is sent to the local scheduler first
      false /* has_more */);
}

void Sequencer::MaybeSendDelayedBatches() {
  for (auto itr = delayed_batches_.begin(); itr != delayed_batches_.end();) {
    // Create exponential distribution of delay
    if (rand() % config_->GetReplicationDelayAmount() == 0) {
      // Replicate batch to all machines EXCEPT local
      auto req = *itr;
      auto num_partitions = config_->GetNumPartitions();
      auto num_replicas = config_->GetNumReplicas();
      for (uint32_t part = 0; part < num_partitions; part++) {
        for (uint32_t rep = 0; rep < num_replicas; rep++) {
          if (part == config_->GetLocalPartition() && rep == config_->GetLocalReplica()) {
            continue;
          }
          auto machine_id = MakeMachineIdAsString(rep, part);
          Send(
              req,
              machine_id,
              SCHEDULER_CHANNEL,
              // Note: atomicity is lost already, since the batch
              // is sent to the local scheduler first
              false /* has_more */);
        }
      }
      itr = delayed_batches_.erase(itr);
    } else {
      itr++;
    }
  }
}
#endif /* ENABLE_REPLICATION_DELAY */

} // namespace slog