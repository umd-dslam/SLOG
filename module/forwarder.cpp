#include "module/forwarder.h"

#include <glog/logging.h>

#include <algorithm>

#include "common/constants.h"
#include "common/monitor.h"
#include "common/proto_utils.h"

using std::move;
using std::sort;
using std::string;
using std::unique;

namespace slog {

using internal::Envelope;
using internal::Request;
using internal::Response;
namespace {

bool TransactionContainsKey(const Transaction& txn, const Key& key) {
  return txn.read_set().contains(key) || txn.write_set().contains(key);
}

uint32_t ChooseRandomPartition(const Transaction& txn, std::mt19937& rg) {
  std::uniform_int_distribution<> idx(0, txn.internal().involved_partitions_size() - 1);
  return txn.internal().involved_partitions(idx(rg));
}

}  // namespace

Forwarder::Forwarder(const ConfigurationPtr& config, const shared_ptr<Broker>& broker,
                     const shared_ptr<LookupMasterIndex<Key, Metadata>>& lookup_master_index,
                     std::chrono::milliseconds poll_timeout)
    : NetworkedModule("Forwarder", broker, kForwarderChannel, poll_timeout),
      config_(config),
      lookup_master_index_(lookup_master_index),
      rg_(std::random_device()()) {}

void Forwarder::HandleInternalRequest(EnvelopePtr&& env) {
  switch (env->request().type_case()) {
    case Request::kForwardTxn:
      ProcessForwardTxn(move(env));
      break;
    case Request::kLookupMaster:
      ProcessLookUpMasterRequest(move(env));
      break;
    default:
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(env->request().type_case(), Request) << "\"";
  }
}

void Forwarder::ProcessForwardTxn(EnvelopePtr&& env) {
  auto txn = env->mutable_request()->mutable_forward_txn()->mutable_txn();

  TRACE(txn->mutable_internal(), TransactionEvent::ENTER_FORWARDER);

  auto local_partition = config_->local_partition();

  // Prepare a lookup master request just in case
  Envelope lookup_env;
  auto lookup_master = lookup_env.mutable_request()->mutable_lookup_master();
  vector<uint32_t> involved_partitions;

  // This function will be called on the read and write set of the current txn
  auto LocalMasterLookupFn = [this, txn, local_partition, lookup_master,
                              &involved_partitions](const google::protobuf::Map<string, string>& keys) {
    auto txn_metadata = txn->mutable_internal()->mutable_master_metadata();
    lookup_master->set_txn_id(txn->internal().id());
    for (auto& pair : keys) {
      const auto& key = pair.first;
      uint32_t partition = 0;

      try {
        partition = config_->partition_of_key(key);
      } catch (std::invalid_argument& e) {
        LOG(ERROR) << "Only numeric keys are allowed while running in Simple Partitioning mode";
        return;
      }

      involved_partitions.push_back(partition);

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

  sort(involved_partitions.begin(), involved_partitions.end());
  auto last = unique(involved_partitions.begin(), involved_partitions.end());
  *txn->mutable_internal()->mutable_involved_partitions() = {involved_partitions.begin(), last};

  // If there is no need to look master info from remote partitions,
  // forward the txn immediately
  if (lookup_master->keys().empty()) {
    auto txn_type = SetTransactionType(*txn);
    VLOG(3) << "Determine txn " << txn->internal().id() << " to be " << ENUM_NAME(txn_type, TransactionType)
            << " without remote master lookup";
    DCHECK(txn_type != TransactionType::UNKNOWN);
    Forward(move(env));
    return;
  }

  VLOG(3) << "Remote master lookup needed to determine type of txn " << txn->internal().id();
  pending_transactions_.insert_or_assign(txn->internal().id(), move(env));

  // Send a look up master request to each partition in the same region
  auto local_rep = config_->local_replica();
  auto num_partitions = config_->num_partitions();
  for (uint32_t part = 0; part < num_partitions; part++) {
    if (part != local_partition) {
      Send(lookup_env, config_->MakeMachineId(local_rep, part), kForwarderChannel);
    }
  }
}

void Forwarder::ProcessLookUpMasterRequest(EnvelopePtr&& env) {
  const auto& lookup_master = env->request().lookup_master();
  Envelope lookup_env;
  auto lookup_response = lookup_env.mutable_response()->mutable_lookup_master();
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
  Send(lookup_env, env->from(), kForwarderChannel);
}

void Forwarder::HandleInternalResponse(EnvelopePtr&& env) {
  // The forwarder only cares about lookup master responses
  if (env->response().type_case() != Response::kLookupMaster) {
    LOG(ERROR) << "Unexpected response type received: \"" << CASE_NAME(env->response().type_case(), Response) << "\"";
  }

  const auto& lookup_master = env->response().lookup_master();
  auto txn_id = lookup_master.txn_id();
  auto pending_txn_it = pending_transactions_.find(txn_id);
  if (pending_txn_it == pending_transactions_.end()) {
    return;
  }

  // Transfer master info from the lookup response to its intended transaction
  auto& pending_env = pending_txn_it->second;
  auto txn = pending_env->mutable_request()->mutable_forward_txn()->mutable_txn();
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

  auto txn_type = SetTransactionType(*txn);
  if (txn_type != TransactionType::UNKNOWN) {
    VLOG(3) << "Determine txn " << txn->internal().id() << " to be " << ENUM_NAME(txn_type, TransactionType);
    Forward(move(pending_env));
    pending_transactions_.erase(txn_id);
  }
}

void Forwarder::Forward(EnvelopePtr&& env) {
  auto txn = env->mutable_request()->mutable_forward_txn()->mutable_txn();
  auto txn_internal = txn->mutable_internal();
  auto txn_id = txn_internal->id();
  auto txn_type = txn_internal->type();
  auto& master_metadata = txn_internal->master_metadata();

  if (txn_type == TransactionType::SINGLE_HOME) {
    // If this current replica is its home, forward to the sequencer of the same machine
    // Otherwise, forward to the sequencer of a random machine in its home region
    auto home_replica = master_metadata.begin()->second.master();
    if (home_replica == config_->local_replica()) {
      VLOG(3) << "Current region is home of txn " << txn_id;

      TRACE(txn_internal, TransactionEvent::EXIT_FORWARDER_TO_SEQUENCER);

      Send(move(env), kSequencerChannel);
    } else {
      auto partition = ChooseRandomPartition(*txn, rg_);
      auto random_machine_in_home_replica = config_->MakeMachineId(home_replica, partition);

      VLOG(3) << "Forwarding txn " << txn_id << " to its home region (rep: " << home_replica << ", part: " << partition
              << ")";

      TRACE(txn_internal, TransactionEvent::EXIT_FORWARDER_TO_SEQUENCER);

      Send(*env, random_machine_in_home_replica, kSequencerChannel);
    }
  } else if (txn_type == TransactionType::MULTI_HOME) {
    VLOG(3) << "Txn " << txn_id << " is a multi-home txn. Sending to the orderer.";

    // Compute involved replicas and store in the txn
    vector<uint32_t> involved_replicas;
    for (const auto& pair : master_metadata) {
      involved_replicas.push_back(pair.second.master());
    }
#ifdef REMASTER_PROTOCOL_COUNTERLESS
    if (txn->procedure_case() == Transaction::kRemaster) {
      involved_replicas.push_back(txn->remaster().new_master());
    }
#endif

    sort(involved_replicas.begin(), involved_replicas.end());
    auto last = unique(involved_replicas.begin(), involved_replicas.end());
    *txn_internal->mutable_involved_replicas() = {involved_replicas.begin(), last};

    TRACE(txn_internal, TransactionEvent::EXIT_FORWARDER_TO_MULTI_HOME_ORDERER);

    if (config_->bypass_mh_orderer()) {
      // Send the txn to sequencers of involved replicas to generate lock-only txns
      auto part = ChooseRandomPartition(*txn, rg_);
      for (auto rep : txn_internal->involved_replicas()) {
        Send(*env, config_->MakeMachineId(rep, part), kSequencerChannel);
      }
      // Send the txn to schedulers of all partitions
      bool send_local = false;
      for (uint32_t part = 0; part < config_->num_partitions(); part++) {
        for (uint32_t rep = 0; rep < config_->num_replicas(); rep++) {
          auto machine_id = config_->MakeMachineId(rep, part);
          if (machine_id == config_->local_machine_id()) {
            send_local = true;
          } else {
            Send(*env, machine_id, kSchedulerChannel);
          }
        }
      }
      if (send_local) {
        Send(move(env), kSchedulerChannel);
      }
    } else {
      auto mh_orderer =
          config_->MakeMachineId(config_->local_replica(), config_->leader_partition_for_multi_home_ordering());
      Send(*env, mh_orderer, kMultiHomeOrdererChannel);
    }
  }
}

}  // namespace slog