#include "module/forwarder.h"

#include <glog/logging.h>

#include <algorithm>

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

Forwarder::Forwarder(const shared_ptr<Broker>& broker,
                     const shared_ptr<LookupMasterIndex<Key, Metadata>>& lookup_master_index,
                     const MetricsRepositoryManagerPtr& metrics_manager, milliseconds poll_timeout)
    : NetworkedModule("Forwarder", broker, kForwarderChannel, metrics_manager, poll_timeout),
      config_(broker->config()),
      lookup_master_index_(lookup_master_index),
      batch_size_(0),
      rg_(std::random_device()()),
      collecting_stats_(false) {
  partitioned_lookup_request_.resize(config_->num_partitions());
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
    PopulateInvolvedPartitions(config_, *txn);
  } catch (std::invalid_argument& e) {
    LOG(ERROR) << "Only numeric keys are allowed while running in Simple Partitioning mode";
    return;
  }

  bool need_remote_lookup = false;
  for (auto& [key, value] : *txn->mutable_keys()) {
    auto partition = config_->partition_of_key(key);

    // If this is a local partition, lookup the master info from the local storage
    if (partition == config_->local_partition()) {
      Metadata metadata;
      if (lookup_master_index_->GetMasterMetadata(key, metadata)) {
        value.mutable_metadata()->set_master(metadata.master);
        value.mutable_metadata()->set_counter(metadata.counter);
      } else {
        value.mutable_metadata()->set_master(DEFAULT_MASTER_REGION_OF_NEW_KEY);
        value.mutable_metadata()->set_counter(0);
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
    if (p != config_->local_partition()) {
      partitioned_lookup_request_[p].mutable_request()->mutable_lookup_master()->add_txn_ids(txn->internal().id());
    }
  }
  pending_transactions_.insert_or_assign(txn->internal().id(), move(env));

  ++batch_size_;

  // If this is the first txn in the batch, schedule to send the batch at a later time
  if (batch_size_ == 1) {
    NewTimedCallback(config_->forwarder_batch_duration(), [this]() { SendLookupMasterRequestBatch(); });

    batch_starting_time_ = steady_clock::now();
  }

  // Batch size is larger than the maximum size, send the batch immediately
  auto max_batch_size = config_->forwarder_max_batch_size();
  if (max_batch_size > 0 && batch_size_ >= max_batch_size) {
    ClearTimedCallbacks();
    SendLookupMasterRequestBatch();
  }
}

void Forwarder::SendLookupMasterRequestBatch() {
  if (collecting_stats_) {
    stat_batch_sizes_.push_back(batch_size_);
    stat_batch_durations_ms_.push_back((steady_clock::now() - batch_starting_time_).count() / 1000000.0);
  }

  auto local_rep = config_->local_replica();
  auto num_partitions = config_->num_partitions();
  for (uint32_t part = 0; part < num_partitions; part++) {
    if (!partitioned_lookup_request_[part].request().lookup_master().txn_ids().empty()) {
      Send(partitioned_lookup_request_[part], config_->MakeMachineId(local_rep, part), kForwarderChannel);
      partitioned_lookup_request_[part].Clear();
    }
  }
  batch_size_ = 0;
}

void Forwarder::ProcessLookUpMasterRequest(EnvelopePtr&& env) {
  const auto& lookup_master = env->request().lookup_master();
  Envelope lookup_env;
  auto lookup_response = lookup_env.mutable_response()->mutable_lookup_master();
  auto metadata_map = lookup_response->mutable_master_metadata();

  lookup_response->mutable_txn_ids()->CopyFrom(lookup_master.txn_ids());
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
        // Otherwise, assign it to the default region for new key
        auto& new_metadata = (*metadata_map)[key];
        new_metadata.set_master(DEFAULT_MASTER_REGION_OF_NEW_KEY);
        new_metadata.set_counter(0);
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
  for (auto txn_id : lookup_master.txn_ids()) {
    auto pending_txn_it = pending_transactions_.find(txn_id);
    if (pending_txn_it == pending_transactions_.end()) {
      continue;
    }

    // Transfer master info from the lookup response to its intended transaction
    auto& pending_env = pending_txn_it->second;
    auto txn = pending_env->mutable_request()->mutable_forward_txn()->mutable_txn();
    for (auto& [key, value] : *txn->mutable_keys()) {
      if (!value.has_metadata()) {
        auto it = lookup_master.master_metadata().find(key);
        if (it != lookup_master.master_metadata().end()) {
          value.mutable_metadata()->CopyFrom(it->second);
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
    auto home_replica = txn->keys().begin()->second.metadata().master();
    if (home_replica == config_->local_replica()) {
      VLOG(3) << "Current region is home of txn " << txn_id;

      RECORD(txn_internal, TransactionEvent::EXIT_FORWARDER_TO_SEQUENCER);

      Send(move(env), kSequencerChannel);
    } else {
      auto partition = ChooseRandomPartition(*txn, rg_);
      auto random_machine_in_home_replica = config_->MakeMachineId(home_replica, partition);

      VLOG(3) << "Forwarding txn " << txn_id << " to its home region (rep: " << home_replica << ", part: " << partition
              << ")";

      RECORD(txn_internal, TransactionEvent::EXIT_FORWARDER_TO_SEQUENCER);

      Send(*env, random_machine_in_home_replica, kSequencerChannel);
    }
  } else if (txn_type == TransactionType::MULTI_HOME_OR_LOCK_ONLY) {
    VLOG(3) << "Txn " << txn_id << " is a multi-home txn. Sending to the orderer.";

    RECORD(txn_internal, TransactionEvent::EXIT_FORWARDER_TO_MULTI_HOME_ORDERER);

    if (config_->bypass_mh_orderer()) {
      auto part = ChooseRandomPartition(*txn, rg_);

      if (config_->synchronized_batching()) {
        // If synchronized batching is on, compute the batching delay based on latency between the current
        // replica to the involved replicas
        uint32_t max_latency = 0;
        for (auto rep : txn_internal->involved_replicas()) {
          max_latency = std::max(max_latency, config_->latency(rep));
        }
        // Send the txn directly to sequencers of involved replicas to generate lock-only txns
        auto txn = env->mutable_request()->mutable_forward_txn()->mutable_txn();
        for (auto rep : txn_internal->involved_replicas()) {
          auto delay = max_latency - config_->latency(rep);
          txn->mutable_internal()->set_sequencer_delay_ms(delay);
          Send(*env, config_->MakeMachineId(rep, part), kSequencerChannel);
        }
      } else {
        // Send the txn directly to sequencers of involved replicas to generate lock-only txns
        vector<MachineId> destinations;
        destinations.reserve(txn_internal->involved_replicas_size());
        for (auto rep : txn_internal->involved_replicas()) {
          destinations.push_back(config_->MakeMachineId(rep, part));
        }
        Send(*env, destinations, kSequencerChannel);
      }
    } else {
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