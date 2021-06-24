#include "module/forwarder.h"

#include <glog/logging.h>

#include <algorithm>
#include <unordered_map>

#include "common/constants.h"
#include "common/json_utils.h"
#include "common/proto_utils.h"

using std::move;
using std::shared_ptr;
using std::sort;
using std::string;
using std::unique;

namespace slog {

using internal::Envelope;
using internal::Request;
using internal::Response;
namespace {

uint32_t ChooseRandomPartition(const Transaction& txn, std::mt19937& rg) {
  std::uniform_int_distribution<> idx(0, txn.internal().involved_partitions_size() - 1);
  return txn.internal().involved_partitions(idx(rg));
}

}  // namespace

Forwarder::Forwarder(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
                     const shared_ptr<LookupMasterIndex>& lookup_master_index,
                     const std::shared_ptr<MetadataInitializer>& metadata_initializer,
                     const MetricsRepositoryManagerPtr& metrics_manager, std::chrono::milliseconds poll_timeout)
    : NetworkedModule(context, config, config->forwarder_port(), kForwarderChannel, metrics_manager, poll_timeout),
      sharder_(Sharder::MakeSharder(config)),
      lookup_master_index_(lookup_master_index),
      metadata_initializer_(metadata_initializer),
      batch_size_(0),
      rg_(std::random_device()()),
      collecting_stats_(false) {
  partitioned_lookup_request_.resize(config->num_partitions());
}

void Forwarder::OnInternalRequestReceived(EnvelopePtr&& env) {
  switch (env->request().type_case()) {
    case Request::kForwardTxn:
      ProcessForwardTxn(move(env));
      break;
    case Request::kLookupMaster:
      ProcessLookUpMasterRequest(move(env));
      break;
    case Request::kStats:
      ProcessStatsRequest(env->request().stats());
      break;
    default:
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(env->request().type_case(), Request) << "\"";
  }
}

void Forwarder::ProcessForwardTxn(EnvelopePtr&& env) {
  auto txn = env->mutable_request()->mutable_forward_txn()->mutable_txn();

  RECORD(txn->mutable_internal(), TransactionEvent::ENTER_FORWARDER);

  try {
    PopulateInvolvedPartitions(sharder_, *txn);
  } catch (std::invalid_argument& e) {
    LOG(ERROR) << "Only numeric keys are allowed while running in Simple Partitioning mode";
    return;
  }

  bool need_remote_lookup = false;
  for (auto& kv : *txn->mutable_keys()) {
    const auto& key = kv.key();
    auto value = kv.mutable_value_entry();
    auto partition = sharder_->compute_partition(key);

    // If this is a local partition, lookup the master info from the local storage
    if (partition == config()->local_partition()) {
      Metadata metadata;
      if (lookup_master_index_->GetMasterMetadata(key, metadata)) {
        value->mutable_metadata()->set_master(metadata.master);
        value->mutable_metadata()->set_counter(metadata.counter);
      } else {
        auto metadata = metadata_initializer_->Compute(key);
        value->mutable_metadata()->set_master(metadata.master);
        value->mutable_metadata()->set_counter(metadata.counter);
      }
    } else {
      // Otherwise, add the key to the appropriate remote lookup master request
      partitioned_lookup_request_[partition].mutable_request()->mutable_lookup_master()->add_keys(key);
      need_remote_lookup = true;
    }
  }

  // If there is no need to look master info from remote partitions,
  // forward the txn immediately
  if (!need_remote_lookup) {
    auto txn_type = SetTransactionType(*txn);
    VLOG(3) << "Determine txn " << txn->internal().id() << " to be " << ENUM_NAME(txn_type, TransactionType)
            << " without remote master lookup";
    DCHECK(txn_type != TransactionType::UNKNOWN);
    Forward(move(env));
    return;
  }

  VLOG(3) << "Remote master lookup needed to determine type of txn " << txn->internal().id();
  for (auto p : txn->internal().involved_partitions()) {
    if (p != config()->local_partition()) {
      partitioned_lookup_request_[p].mutable_request()->mutable_lookup_master()->add_txn_ids(txn->internal().id());
    }
  }
  pending_transactions_.insert_or_assign(txn->internal().id(), move(env));

  ++batch_size_;

  // If this is the first txn in the batch, schedule to send the batch at a later time
  if (batch_size_ == 1) {
    NewTimedCallback(config()->forwarder_batch_duration(), [this]() { SendLookupMasterRequestBatch(); });

    batch_starting_time_ = std::chrono::steady_clock::now();
  }
}

void Forwarder::SendLookupMasterRequestBatch() {
  if (collecting_stats_) {
    stat_batch_sizes_.push_back(batch_size_);
    stat_batch_durations_ms_.push_back((std::chrono::steady_clock::now() - batch_starting_time_).count() / 1000000.0);
  }

  auto local_rep = config()->local_replica();
  auto num_partitions = config()->num_partitions();
  for (uint32_t part = 0; part < num_partitions; part++) {
    if (!partitioned_lookup_request_[part].request().lookup_master().txn_ids().empty()) {
      Send(partitioned_lookup_request_[part], config()->MakeMachineId(local_rep, part), kForwarderChannel);
      partitioned_lookup_request_[part].Clear();
    }
  }
  batch_size_ = 0;
}

void Forwarder::ProcessLookUpMasterRequest(EnvelopePtr&& env) {
  const auto& lookup_master = env->request().lookup_master();
  Envelope lookup_env;
  auto lookup_response = lookup_env.mutable_response()->mutable_lookup_master();
  auto results = lookup_response->mutable_lookup_results();

  lookup_response->mutable_txn_ids()->CopyFrom(lookup_master.txn_ids());
  for (int i = 0; i < lookup_master.keys_size(); i++) {
    const auto& key = lookup_master.keys(i);

    if (sharder_->is_local_key(key)) {
      if (Metadata metadata; lookup_master_index_->GetMasterMetadata(key, metadata)) {
        // If key exists, add the metadata of current key to the response
        auto key_metadata = results->Add();
        key_metadata->set_key(key);
        key_metadata->mutable_metadata()->set_master(metadata.master);
        key_metadata->mutable_metadata()->set_counter(metadata.counter);
      } else {
        // Otherwise, assign it to the default region for new key
        auto key_metadata = results->Add();
        key_metadata->set_key(key);
        auto new_metadata = metadata_initializer_->Compute(key);
        key_metadata->mutable_metadata()->set_master(new_metadata.master);
        key_metadata->mutable_metadata()->set_counter(new_metadata.counter);
      }
    }
  }
  Send(lookup_env, env->from(), kForwarderChannel);
}

void Forwarder::OnInternalResponseReceived(EnvelopePtr&& env) {
  // The forwarder only cares about lookup master responses
  if (env->response().type_case() != Response::kLookupMaster) {
    LOG(ERROR) << "Unexpected response type received: \"" << CASE_NAME(env->response().type_case(), Response) << "\"";
  }

  const auto& lookup_master = env->response().lookup_master();
  std::unordered_map<std::string, int> index;
  for (int i = 0; i < lookup_master.lookup_results_size(); i++) {
    index[lookup_master.lookup_results(i).key()] = i;
  }

  for (auto txn_id : lookup_master.txn_ids()) {
    auto pending_txn_it = pending_transactions_.find(txn_id);
    if (pending_txn_it == pending_transactions_.end()) {
      continue;
    }

    // Transfer master info from the lookup response to its intended transaction
    auto& pending_env = pending_txn_it->second;
    auto txn = pending_env->mutable_request()->mutable_forward_txn()->mutable_txn();
    for (auto& kv : *txn->mutable_keys()) {
      if (!kv.value_entry().has_metadata()) {
        auto it = index.find(kv.key());
        if (it != index.end()) {
          const auto& result = lookup_master.lookup_results(it->second).metadata();
          kv.mutable_value_entry()->mutable_metadata()->CopyFrom(result);
        }
      }
    }

    auto txn_type = SetTransactionType(*txn);
    if (txn_type != TransactionType::UNKNOWN) {
      VLOG(3) << "Determine txn " << txn->internal().id() << " to be " << ENUM_NAME(txn_type, TransactionType);
      Forward(move(pending_env));
      pending_transactions_.erase(txn_id);
    }
  }
}

void Forwarder::Forward(EnvelopePtr&& env) {
  auto txn = env->mutable_request()->mutable_forward_txn()->mutable_txn();
  auto txn_internal = txn->mutable_internal();
  auto txn_id = txn_internal->id();
  auto txn_type = txn_internal->type();

  PopulateInvolvedReplicas(*txn);

  if (txn_type == TransactionType::SINGLE_HOME) {
    // If this current replica is its home, forward to the sequencer of the same machine
    // Otherwise, forward to the sequencer of a random machine in its home region
    auto home_replica = txn->keys().begin()->value_entry().metadata().master();
    if (home_replica == config()->local_replica()) {
      VLOG(3) << "Current region is home of txn " << txn_id;

      RECORD(txn_internal, TransactionEvent::EXIT_FORWARDER_TO_SEQUENCER);

      Send(move(env), kSequencerChannel);
    } else {
      auto partition = ChooseRandomPartition(*txn, rg_);
      auto random_machine_in_home_replica = config()->MakeMachineId(home_replica, partition);

      VLOG(3) << "Forwarding txn " << txn_id << " to its home region (rep: " << home_replica << ", part: " << partition
              << ")";

      RECORD(txn_internal, TransactionEvent::EXIT_FORWARDER_TO_SEQUENCER);

      Send(*env, random_machine_in_home_replica, kSequencerChannel);
    }
  } else if (txn_type == TransactionType::MULTI_HOME_OR_LOCK_ONLY) {
    RECORD(txn_internal, TransactionEvent::EXIT_FORWARDER_TO_MULTI_HOME_ORDERER);

    if (config()->bypass_mh_orderer()) {
      VLOG(3) << "Txn " << txn_id << " is a multi-home txn. Sending to the sequencer.";
      auto part = ChooseRandomPartition(*txn, rg_);
      // Send the txn directly to sequencers of involved replicas to generate lock-only txns
      std::vector<MachineId> destinations;
      destinations.reserve(txn_internal->involved_replicas_size());
      for (auto rep : txn_internal->involved_replicas()) {
        destinations.push_back(config()->MakeMachineId(rep, part));
      }
      Send(*env, destinations, kSequencerChannel);
    } else {
      VLOG(3) << "Txn " << txn_id << " is a multi-home txn. Sending to the orderer.";
      // Send the txn to the orderer to form a global order
      Send(move(env), kMultiHomeOrdererChannel);
    }
  }
}

/**
 * {
 *    forw_batch_size_pctls:        [int],
 *    forw_batch_duration_ms_pctls: [float]
 * }
 */
void Forwarder::ProcessStatsRequest(const internal::StatsRequest& stats_request) {
  using rapidjson::StringRef;

  int level = stats_request.level();

  rapidjson::Document stats;
  stats.SetObject();
  auto& alloc = stats.GetAllocator();

  if (level == 0) {
    collecting_stats_ = false;
  } else if (level > 0) {
    collecting_stats_ = true;
  }

  stats.AddMember(StringRef(FORW_BATCH_SIZE_PCTLS), Percentiles(stat_batch_sizes_, alloc), alloc);
  stat_batch_sizes_.clear();

  stats.AddMember(StringRef(FORW_BATCH_DURATION_MS_PCTLS), Percentiles(stat_batch_durations_ms_, alloc), alloc);
  stat_batch_durations_ms_.clear();

  // Write JSON object to a buffer and send back to the server
  rapidjson::StringBuffer buf;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
  stats.Accept(writer);

  auto env = NewEnvelope();
  env->mutable_response()->mutable_stats()->set_id(stats_request.id());
  env->mutable_response()->mutable_stats()->set_stats_json(buf.GetString());
  Send(move(env), kServerChannel);
}

}  // namespace slog