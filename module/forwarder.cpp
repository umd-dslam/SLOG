#include "module/forwarder.h"

#include <glog/logging.h>

#include "common/constants.h"
#include "common/proto_utils.h"

namespace slog {

Forwarder::Forwarder(
    shared_ptr<Configuration> config,
    Channel* listener) 
  : BasicModule(listener),
    config_(config),
    re_(rd_()) {
}

void Forwarder::HandleInternalRequest(
    internal::Request&& req,
    string&& /* from_machine_id */,
    string&& /* from_channel */) {
  if (req.type_case() != internal::Request::kForward) {
    return;
  }
  const auto& txn = req.forward().txn();
  if (txn.internal().type() == TransactionType::UNKNOWN) {
    pending_transaction_[txn.id()] = txn;
    // Send a look up master request for each partition in the same region
    internal::Request lookup_master_request;
    FillLookupMasterRequest(lookup_master_request, txn);
    auto rep = config_->GetLocalReplica();
    for (uint32_t part = 0; part < config_->GetNumPartitions(); part++) {
      Send(lookup_master_request, MakeMachineId(rep, part), SERVER_CHANNEL);
    }
  } else {
    Send(req, SEQUENCER_CHANNEL);
  }
}

void Forwarder::HandleInternalResponse(
    internal::Response&& res,
    string&& /* from_machine_id */,
    string&& /* from_channel */) {
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
    if (txn.read_set().contains(pair.first) || txn.write_set().contains(pair.first)) {
      txn_master_metadata->insert(pair);
    }
  }

  auto total_num_keys = static_cast<size_t>(
      txn.read_set_size() + txn.write_set_size());
  // If the transaction has all master info it needs, 
  // forward it to the appropriate sequencer
  if (txn_master_metadata->size() == total_num_keys) {
    Forward(txn);
    pending_transaction_.erase(txn_id);
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
  lookup_master->set_txn_id(txn.id());
}

void Forwarder::Forward(const Transaction& txn) {
  auto& txn_master_metadata = txn.internal().master_metadata();

  bool is_single_home = true;
  const auto& any_key = txn_master_metadata.begin()->first;
  for (const auto& pair : txn_master_metadata) {
    if (pair.first != any_key) {
      is_single_home = false;
      break;
    }
  }

  // Prepare a request to be forwarded to a sequencer
  internal::Request forward_request;
  auto forwarded_txn = forward_request.mutable_forward()->mutable_txn();
  forwarded_txn->CopyFrom(txn);

  if (is_single_home) {
    forwarded_txn
        ->mutable_internal()
        ->set_type(TransactionType::SINGLE_HOME);

    // Get master of the first key since it is the same for all keys
    auto home_replica = txn_master_metadata.at(any_key).master();
    // If this current replica is its home, forward to the sequencer of the same machine
    // Otherwise, forward to the sequencer of a random machine in its home region
    if (home_replica == config_->GetLocalReplica()) {
      Send(forward_request, SEQUENCER_CHANNEL);
    } else {
      std::uniform_int_distribution<> dist(0, config_->GetNumPartitions() - 1);
      auto random_machine_in_home_replica = MakeMachineId(home_replica, dist(re_));
      Send(
          forward_request, random_machine_in_home_replica, SCHEDULER_CHANNEL);
    }
  } else {
    forwarded_txn
        ->mutable_internal()
        ->set_type(TransactionType::MULTI_HOME);
    // TODO: send to a multi-home transaction ordering module
  }
}



} // namespace slog