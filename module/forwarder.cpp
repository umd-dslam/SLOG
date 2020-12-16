#include "module/forwarder.h"

#include <glog/logging.h>

#include "common/constants.h"
#include "common/proto_utils.h"

using std::move;

namespace slog {

using internal::Request;
using internal::Response;

namespace {

inline bool TransactionContainsKey(const Transaction& txn, const Key& key) {
  return txn.read_set().contains(key) || txn.write_set().contains(key);
}

} // namespace

Forwarder::Forwarder(
    const ConfigurationPtr& config,
    const shared_ptr<Broker>& broker,
    const shared_ptr<LookupMasterIndex<Key, Metadata>>& lookup_master_index,
    int poll_timeout_ms)
  : NetworkedModule("Forwarder", broker, kForwarderChannel, poll_timeout_ms),
    config_(config),
    lookup_master_index_(lookup_master_index),
    rg_(std::random_device()()) {}

void Forwarder::HandleInternalRequest(ReusableRequest&& req, MachineId from) {
  switch (req.get()->type_case()) {
    case internal::Request::kForwardTxn:
      ProcessForwardTxn(move(req));
      break;
    case internal::Request::kLookupMaster:
      ProcessLookUpMasterRequest(move(req), from);
      break;
    default:
      LOG(ERROR) << "Unexpected request type received: \""
                << CASE_NAME(req.get()->type_case(), Request) << "\"";
    }
}

void Forwarder::ProcessForwardTxn(ReusableRequest&& req) {
  auto txn = req.get()->mutable_forward_txn()->mutable_txn();
  RecordTxnEvent(
      config_,
      txn->mutable_internal(),
      TransactionEvent::ENTER_FORWARDER);

  auto local_partition = config_->local_partition();

  // Prepare a lookup master request just in case
  auto lookup_master_request = NewRequest();
  auto lookup_master = lookup_master_request.get()->mutable_lookup_master();

  // This function will be called on the read and write set of the current txn
  auto LocalMasterLookupFn = [this, txn, local_partition, lookup_master](
      const google::protobuf::Map<std::string, std::string>& keys) {
    auto partitions = txn->mutable_internal()->mutable_partitions();
    auto txn_metadata = txn->mutable_internal()->mutable_master_metadata();
    lookup_master->set_txn_id(txn->internal().id());
    for (auto& pair : keys) {
      const auto& key = pair.first;
      auto partition = config_->partition_of_key(key);

      // Add the partition of this key to the partition list of current txn if not exist
      if (std::find(partitions->begin(), partitions->end(), partition) == partitions->end()) {
        partitions->Add(partition);
      }

      // If this is a local partition, lookup the master info from the local storage
      if (partition == local_partition) {
        auto& new_metadata = (*txn_metadata)[key];
        Metadata metadata;
        if (lookup_master_index_->GetMasterMetadata(key, metadata)) {
          new_metadata.set_master(metadata.master);
          new_metadata.set_counter(metadata.counter);
        } else {
          new_metadata.set_master(DEFAULT_MASTER_REGION_OF_NEW_KEY);
          new_metadata.set_counter(0);
        }
      } else {
        // Otherwise, add the key to the remote lookup master request
        lookup_master->add_keys(key);
      }
    }
  };

  LocalMasterLookupFn(txn->read_set());
  LocalMasterLookupFn(txn->write_set());

  // If there is no need to look master info from remote partitions,
  // forward the txn immediately
  if (lookup_master->keys().empty()) {
    auto txn_type = SetTransactionType(*txn);
    if (txn_type != TransactionType::UNKNOWN) {
      Forward(txn);
    }
    return;
  }

  pending_transactions_[txn->internal().id()] = move(req);

  // Send a look up master request to each partition in the same region
  auto local_rep = config_->local_replica();
  auto num_partitions = config_->num_partitions();
  for (uint32_t part = 0; part < num_partitions; part++) {
    if (part != local_partition) {
      Send(
          *lookup_master_request.get(),
          kForwarderChannel,
          config_->MakeMachineId(local_rep, part));
    }
  }
}

void Forwarder::ProcessLookUpMasterRequest(ReusableRequest&& req, MachineId from) {
  const auto& lookup_master = req.get()->lookup_master();
  internal::Response response;
  auto lookup_response = response.mutable_lookup_master();
  auto metadata_map = lookup_response->mutable_master_metadata();

  lookup_response->set_txn_id(lookup_master.txn_id());
  for (int i = 0; i < lookup_master.keys_size(); i++) {
    const auto& key = lookup_master.keys(i);

    if (config_->key_is_in_local_partition(key)) {
      Metadata metadata;
      if (lookup_master_index_->GetMasterMetadata(key, metadata)) {
        // If key exists, add the metadata of current key to the response
        auto& response_metadata = (*metadata_map)[key];
        response_metadata.set_master(metadata.master);
        response_metadata.set_counter(metadata.counter);
      } else {
        // Otherwise, add it to the list indicating this is a new key
        lookup_response->add_new_keys(key);
      }
    }
  }
  Send(response, kForwarderChannel, from);
}

void Forwarder::HandleInternalResponse(ReusableResponse&& res, MachineId /* from */) {
  // The forwarder only cares about lookup master responses
  if (res.get()->type_case() != Response::kLookupMaster) {
    LOG(ERROR) << "Unexpected response type received: \""
               << CASE_NAME(res.get()->type_case(), Response) << "\"";
  }

  const auto& lookup_master = res.get()->lookup_master();
  auto txn_id = lookup_master.txn_id();
  if (pending_transactions_.count(txn_id) == 0) {
    return;
  }

  // Transfer master info from the lookup response to its intended transaction
  auto txn = pending_transactions_[txn_id].get()->mutable_forward_txn()->mutable_txn();
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
    pending_transactions_.erase(txn_id);
  }
}

void Forwarder::Forward(Transaction* txn) {
  auto txn_internal = txn->mutable_internal();
  auto txn_id = txn_internal->id();
  auto txn_type = txn_internal->type();
  auto& master_metadata = txn_internal->master_metadata();

  // Prepare a request to be forwarded to a sequencer
  Request forward_txn;
  // The transaction still belongs to the request. This is only temporary and will
  // be released at the end
  forward_txn.mutable_forward_txn()->set_allocated_txn(txn);

  if (txn_type == TransactionType::SINGLE_HOME) {
    // If this current replica is its home, forward to the sequencer of the same machine
    // Otherwise, forward to the sequencer of a random machine in its home region
    auto home_replica = master_metadata.begin()->second.master();
    if (home_replica == config_->local_replica()) {
      VLOG(3) << "Current region is home of txn " << txn_id;
      RecordTxnEvent(
          config_,
          txn_internal,
          TransactionEvent::EXIT_FORWARDER_TO_SEQUENCER);
      Send(forward_txn, kSequencerChannel);
    } else {
      std::uniform_int_distribution<> RandomPartition(0, config_->num_partitions() - 1);
      auto partition = RandomPartition(rg_);
      auto random_machine_in_home_replica = config_->MakeMachineId(home_replica, partition);

      VLOG(3) << "Forwarding txn " << txn_id << " to its home region (rep: "
              << home_replica << ", part: " << partition << ")";

      RecordTxnEvent(
          config_,
          txn_internal,
          TransactionEvent::EXIT_FORWARDER_TO_SEQUENCER);
      Send(
          forward_txn,
          kSequencerChannel,
          random_machine_in_home_replica);
    }
  } else if (txn_type == TransactionType::MULTI_HOME) {
    auto destination = config_->MakeMachineId(
        config_->local_replica(),
        config_->leader_partition_for_multi_home_ordering());

    VLOG(3) << "Txn " << txn_id << " is a multi-home txn. Sending to the orderer.";

    RecordTxnEvent(
        config_,
        txn_internal,
        TransactionEvent::EXIT_FORWARDER_TO_MULTI_HOME_ORDERER);
    Send(
        forward_txn,
        kMultiHomeOrdererChannel,
        destination);
  }
  // Release txn so that it won't be freed by forward_txn and later double-freed
  // by the request
  forward_txn.mutable_forward_txn()->release_txn();
}

} // namespace slog