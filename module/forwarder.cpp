#include "module/forwarder.h"

#include <glog/logging.h>

#include "common/constants.h"
#include "common/proto_utils.h"

namespace slog {

namespace {

bool TransactionContainsKey(const Transaction& txn, const Key& key) {
  return txn.read_set().contains(key) || txn.write_set().contains(key);
}

} // namespace

Forwarder::Forwarder(
    shared_ptr<Configuration> config,
    Broker& broker) 
  : BasicModule(broker.AddChannel(FORWARDER_CHANNEL)),
    config_(config),
    RandomPartition(0, config->GetNumPartitions() - 1) {}

void Forwarder::HandleInternalRequest(
    internal::Request&& req,
    string&& /* from_machine_id */) {
  // The forwarder only cares about Forward requests
  if (req.type_case() != internal::Request::kForwardTxn) {
    return;
  }

  auto txn = req.mutable_forward_txn()->release_txn();
  auto txn_type = SetTransactionType(*txn);
  // Forward the transaction if we already knoww the type of the txn
  if (txn_type != TransactionType::UNKNOWN) {
    Forward(txn);
    return;
  }

  pending_transaction_[txn->internal().id()] = txn;

  // Send a look up master request to each partition in the same region
  internal::Request lookup_master_request;
  FillLookupMasterRequest(lookup_master_request, *txn);
  auto rep = config_->GetLocalReplica();
  for (uint32_t part = 0; part < config_->GetNumPartitions(); part++) {
    Send(
        lookup_master_request,
        MakeMachineId(rep, part),
        SERVER_CHANNEL);
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
  // The forwarder only cares about lookup master responses
  if (res.type_case() != internal::Response::kLookupMaster) {
    return;
  }

  const auto& lookup_master = res.lookup_master();
  auto txn_id = lookup_master.txn_id();
  if (pending_transaction_.count(txn_id) == 0) {
    LOG(ERROR) << "Transaction " << txn_id << " is not waiting for master info";
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
  // The master region of new keys is the one it is first sent to
  // TODO: re-consider this if needed
  for (const auto& new_key : lookup_master.new_keys()) {
    if (TransactionContainsKey(*txn, new_key)) {
      auto& new_metadata = (*txn_master_metadata)[new_key];
      new_metadata.set_master(config_->GetLocalReplica());
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
  auto txn_id = txn->internal().id();
  auto txn_type = txn->internal().type();
  auto& master_metadata = txn->internal().master_metadata();

  // Prepare a request to be forwarded to a sequencer
  internal::Request forward_request;
  forward_request.mutable_forward_txn()->set_allocated_txn(txn);

  if (txn_type == TransactionType::SINGLE_HOME) {
    // If this current replica is its home, forward to the sequencer of the same machine
    // Otherwise, forward to the sequencer of a random machine in its home region
    auto home_replica = master_metadata.begin()->second.master();
    if (home_replica == config_->GetLocalReplica()) {
      SendSameMachine(forward_request, SEQUENCER_CHANNEL);
    } else {
      auto partition = RandomPartition(re_);
      auto random_machine_in_home_replica = MakeMachineId(home_replica, partition);

      DLOG(INFO) << "Forwarding txn " << txn_id << " to its home region (rep: "
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