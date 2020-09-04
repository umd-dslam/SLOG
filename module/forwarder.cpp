#include "module/forwarder.h"

#include <glog/logging.h>

#include "common/constants.h"
#include "common/proto_utils.h"

namespace slog {

using internal::Request;
using internal::Response;

namespace {

bool TransactionContainsKey(const Transaction& txn, const Key& key) {
  return txn.read_set().contains(key) || txn.write_set().contains(key);
}

Request MakeLookupMasterRequest(const Transaction& txn) {
  auto& metadata = txn.internal().master_metadata();
  Request req;
  auto lookup_master = req.mutable_lookup_master();
  lookup_master->set_txn_id(txn.internal().id());
  for (const auto& pair : txn.read_set()) {
    if (!metadata.contains(pair.first)) {
      lookup_master->add_keys(pair.first);
    }
  }
  for (const auto& pair : txn.write_set()) {
    if (!metadata.contains(pair.first)) {
      lookup_master->add_keys(pair.first);
    }
  }
  return req;
}

} // namespace

Forwarder::Forwarder(const ConfigurationPtr& config, const shared_ptr<Broker>& broker) 
  : NetworkedModule(broker, FORWARDER_CHANNEL),
    config_(config),
    rg_(std::random_device()()) {}

void Forwarder::HandleInternalRequest(
    Request&& req,
    string&& /* from_machine_id */) {
  // The forwarder only cares about Forward requests
  if (req.type_case() != Request::kForwardTxn) {
    LOG(ERROR) << "Unexpected request type received: \""
               << CASE_NAME(req.type_case(), Request) << "\"";
    return;
  }

  auto txn = req.mutable_forward_txn()->release_txn();
  RecordTxnEvent(
      config_,
      txn->mutable_internal(),
      TransactionEvent::ENTER_FORWARDER);

  auto txn_type = SetTransactionType(*txn);
  // Forward the transaction if we already knoww the type of the txn
  if (txn_type != TransactionType::UNKNOWN) {
    Forward(txn);
    return;
  }

  pending_transaction_[txn->internal().id()] = txn;

  // Send a look up master request to each partition in the same region
  auto lookup_master_request = MakeLookupMasterRequest(*txn);
  auto rep = config_->GetLocalReplica();
  auto num_partitions = config_->GetNumPartitions();
  for (uint32_t part = 0; part < num_partitions; part++) {
    Send(
        lookup_master_request,
        SERVER_CHANNEL,
        MakeMachineIdAsString(rep, part));
  }
}

void Forwarder::HandleInternalResponse(
    Response&& res,
    string&& /* from_machine_id */) {
  // The forwarder only cares about lookup master responses
  if (res.type_case() != Response::kLookupMaster) {
    LOG(ERROR) << "Unexpected response type received: \""
               << CASE_NAME(res.type_case(), Response) << "\"";
    return;
  }

  const auto& lookup_master = res.lookup_master();
  auto txn_id = lookup_master.txn_id();
  if (pending_transaction_.count(txn_id) == 0) {
    return;
  }

  // Transfer master info from the lookup response to its intended transaction
  auto txn = pending_transaction_[txn_id];
  auto txn_master_metadata = txn->mutable_internal()->mutable_master_metadata();
  for (const auto& pair : lookup_master.master_metadata()) {
    if (TransactionContainsKey(*txn, pair.first)) {
      txn_master_metadata->insert(pair);
    }
  }
  // The master of new keys are set to a default region
  for (const auto& new_key : lookup_master.new_keys()) {
    if (TransactionContainsKey(*txn, new_key)) {
      auto& new_metadata = (*txn_master_metadata)[new_key];
      new_metadata.set_master(DEFAULT_MASTER_REGION_OF_NEW_KEY);
      new_metadata.set_counter(0);
    }
  }

  // If a transaction can be determined to be either SINGLE_HOME or MULTI_HOME,
  // forward it to the appropriate sequencer
  auto txn_type = SetTransactionType(*txn);
  if (txn_type != TransactionType::UNKNOWN) {
    Forward(txn);
    pending_transaction_.erase(txn_id);
  }
}

void Forwarder::Forward(Transaction* txn) {
  auto txn_internal = txn->mutable_internal();
  auto txn_id = txn_internal->id();
  auto txn_type = txn_internal->type();
  auto& master_metadata = txn_internal->master_metadata();

  // Prepare a request to be forwarded to a sequencer
  Request forward_txn;
  forward_txn.mutable_forward_txn()->set_allocated_txn(txn);

  if (txn_type == TransactionType::SINGLE_HOME) {
    // If this current replica is its home, forward to the sequencer of the same machine
    // Otherwise, forward to the sequencer of a random machine in its home region
    auto home_replica = master_metadata.begin()->second.master();
    if (home_replica == config_->GetLocalReplica()) {
      VLOG(3) << "Current region is home of txn " << txn_id;
      RecordTxnEvent(
          config_,
          txn_internal,
          TransactionEvent::EXIT_FORWARDER_TO_SEQUENCER);
      Send(forward_txn, SEQUENCER_CHANNEL);
    } else {
      std::uniform_int_distribution<> RandomPartition(0, config_->GetNumPartitions() - 1);
      auto partition = RandomPartition(rg_);
      auto random_machine_in_home_replica = MakeMachineIdAsString(home_replica, partition);

      VLOG(3) << "Forwarding txn " << txn_id << " to its home region (rep: "
              << home_replica << ", part: " << partition << ")";

      RecordTxnEvent(
          config_,
          txn_internal,
          TransactionEvent::EXIT_FORWARDER_TO_SEQUENCER);
      Send(
          forward_txn,
          SEQUENCER_CHANNEL,
          random_machine_in_home_replica);
    }
  } else if (txn_type == TransactionType::MULTI_HOME) {
    auto destination = MakeMachineIdAsString(
        config_->GetLocalReplica(),
        config_->GetLeaderPartitionForMultiHomeOrdering());

    VLOG(3) << "Txn " << txn_id << " is a multi-home txn. Sending to the orderer.";

    RecordTxnEvent(
        config_,
        txn_internal,
        TransactionEvent::EXIT_FORWARDER_TO_MULTI_HOME_ORDERER);
    Send(
        forward_txn,
        MULTI_HOME_ORDERER_CHANNEL,
        destination);
  }
}

} // namespace slog