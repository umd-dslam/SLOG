#include "module/forwarder.h"

#include <glog/logging.h>

#include "common/constants.h"
#include "common/proto_utils.h"

namespace slog {

Forwarder::Forwarder(
    shared_ptr<Configuration> config,
    Broker& broker) 
  : BasicModule(broker.AddChannel(FORWARDER_CHANNEL)),
    config_(config),
    re_(rd_()) {
}

void Forwarder::HandleInternalRequest(
    internal::Request&& req,
    string&& /* from_machine_id */,
    string&& /* from_channel */) {
  // The forwarder only cares about Forward requests
  if (req.type_case() != internal::Request::kForwardTxn) {
    return;
  }

  auto txn = req.mutable_forward_txn()->mutable_txn();
  auto txn_type = SetTransactionType(*txn);
  if (txn_type == TransactionType::UNKNOWN) {
    pending_transaction_[txn->internal().id()] = *txn;

    internal::Request lookup_master_request;
    FillLookupMasterRequest(lookup_master_request, *txn);

    // Send a look up master request for each partition in the same region
    auto rep = config_->GetLocalReplica();
    for (uint32_t part = 0; part < config_->GetNumPartitions(); part++) {
      Send(
          lookup_master_request,
          MakeMachineId(rep, part),
          SERVER_CHANNEL);
    }
  } else {
    Forward(*txn);
  }
}

void Forwarder::FillLookupMasterRequest(
    internal::Request& req, const Transaction& txn) {
  auto lookup_master = req.mutable_lookup_master();
  for (const auto& pair : txn.read_set()) {
    lookup_master->add_keys(pair.first);
  }
  for (const auto& pair : txn.write_set()) {
    lookup_master->add_keys(pair.first);
  }
  lookup_master->set_txn_id(txn.internal().id());
}

void Forwarder::HandleInternalResponse(
    internal::Response&& res,
    string&& /* from_machine_id */) {
  // The forwarder only cares about lookup master response
  if (res.type_case() != internal::Response::kLookupMaster) {
    return;
  }

  const auto& lookup_master = res.lookup_master();
  auto txn_id = lookup_master.txn_id();
  if (pending_transaction_.count(txn_id) == 0) {
    LOG(ERROR) << "Transaction " << txn_id << " is not pending for master info";
    return;
  }

  // Transfer master info from the lookup response to its intended transaction
  auto& txn = pending_transaction_[txn_id];
  auto txn_master_metadata = txn.mutable_internal()->mutable_master_metadata();
  for (const auto& pair : lookup_master.master_metadata()) {
    if (
        txn.read_set().contains(pair.first) 
        || txn.write_set().contains(pair.first)) {
      txn_master_metadata->insert(pair);
    }
  }

  auto txn_type = SetTransactionType(txn);
  if (txn_type != TransactionType::UNKNOWN) {
    Forward(txn);
    pending_transaction_.erase(txn_id);
  }
}

void Forwarder::Forward(const Transaction& txn) {
  auto txn_type = txn.internal().type();
  auto& master_metadata = txn.internal().master_metadata();
  auto home_replica = master_metadata.begin()->second.master();

  // Prepare a request to be forwarded to a sequencer
  internal::Request forward_request;
  auto forwarded_txn = forward_request.mutable_forward_txn()->mutable_txn();
  forwarded_txn->CopyFrom(txn);

  if (txn_type == TransactionType::SINGLE_HOME) {
    // If this current replica is its home, forward to the sequencer of the same machine
    // Otherwise, forward to the sequencer of a random machine in its home region
    if (home_replica == config_->GetLocalReplica()) {
      Send(forward_request, SEQUENCER_CHANNEL);
    } else {
      std::uniform_int_distribution<> dist(0, config_->GetNumPartitions() - 1);
      auto partition = dist(re_);
      auto random_machine_in_home_replica = MakeMachineId(home_replica, partition);

      DLOG(INFO) << "Forwarding txn " << txn.internal().id() << " to its home region (rep: "
                 << home_replica << ", part: " << partition << ")";

      Send(
          forward_request,
          random_machine_in_home_replica,
          SEQUENCER_CHANNEL);
    }
  } else if (txn_type == TransactionType::MULTI_HOME) {
    // TODO: send to a multi-home transaction ordering module
  }
}



} // namespace slog