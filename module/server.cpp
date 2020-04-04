#include "module/server.h"

#include "common/constants.h"
#include "common/json_utils.h"
#include "common/proto_utils.h"
#include "proto/internal.pb.h"

using std::move;

namespace slog {

Server::Server(
    ConfigurationPtr config,
    zmq::context_t& context,
    Broker& broker,
    shared_ptr<LookupMasterIndex<Key, Metadata>> lookup_master_index)
  : ChannelHolder(broker.AddChannel(SERVER_CHANNEL)), 
    config_(config),
    client_socket_(context, ZMQ_ROUTER),
    lookup_master_index_(lookup_master_index),
    txn_id_counter_(0) {
  client_socket_.setsockopt(ZMQ_LINGER, 0);
  client_socket_.setsockopt(ZMQ_RCVHWM, SERVER_RCVHWM);
  client_socket_.setsockopt(ZMQ_SNDHWM, SERVER_SNDHWM);
  poll_items_.push_back(GetChannelPollItem());
  poll_items_.push_back({ 
      static_cast<void*>(client_socket_),
      0, /* fd */
      ZMQ_POLLIN,
      0 /* revent */});
}

/***********************************************
                SetUp and Loop
***********************************************/

void Server::SetUp() {
  string endpoint = 
      "tcp://*:" + std::to_string(config_->GetServerPort());
  client_socket_.bind(endpoint);
  LOG(INFO) << "Bound Server to: " << endpoint;
}

void Server::Loop() {

  zmq::poll(poll_items_, MODULE_POLL_TIMEOUT_MS);

  if (HasMessageFromChannel()) {
    MMessage msg;
    ReceiveFromChannel(msg);

    if (msg.IsProto<internal::Request>()) {
      internal::Request req;
      msg.GetProto(req);
      auto from_machine_id = msg.GetIdentity();
      string from_channel;
      msg.GetString(from_channel, MM_FROM_CHANNEL);
      HandleInternalRequest(
          move(req),
          move(from_machine_id),
          move(from_channel));
    } else if (msg.IsProto<internal::Response>()) {
      internal::Response res;
      msg.GetProto(res);
      HandleInternalResponse(move(res));
    }
  }

  if (HasMessageFromClient()) {
    MMessage msg(client_socket_);
    if (msg.IsProto<api::Request>()) {
      HandleAPIRequest(move(msg));
    }
  }

  VLOG_EVERY_N(4, 5000/MODULE_POLL_TIMEOUT_MS) << "Server is alive";
}

bool Server::HasMessageFromChannel() const {
  return poll_items_[0].revents & ZMQ_POLLIN;
}

bool Server::HasMessageFromClient() const {
  return poll_items_[1].revents & ZMQ_POLLIN;
}

/***********************************************
                  API Requests
***********************************************/

void Server::HandleAPIRequest(MMessage&& msg) {
  api::Request request;
  if (!msg.GetProto(request)) {
    LOG(ERROR) << "Invalid request from client";
    return;
  }

  // While this is called txn id, we use it for any kind of request
  auto txn_id = NextTxnId();
  CHECK(pending_responses_.count(txn_id) == 0) << "Duplicate transaction id: " << txn_id;

  // The message object holds the address of the client so we keep it here
  // to response to the client later
  pending_responses_[txn_id].response = msg;
  // Stream id is used by a client to match up request-response on its side.
  // The server does not use this and just echos it back to the client.
  pending_responses_[txn_id].stream_id = request.stream_id();
  
  switch (request.type_case()) {
    case api::Request::kTxn: {
      auto txn = request.mutable_txn()->release_txn();
      auto txn_internal = txn->mutable_internal();
      txn_internal->set_id(txn_id);
      txn_internal
          ->mutable_coordinating_server()
          ->CopyFrom(
              config_->GetLocalMachineIdAsProto());

      internal::Request forward_request;
      forward_request.mutable_forward_txn()->set_allocated_txn(txn);
      SendSameMachine(forward_request, FORWARDER_CHANNEL);
      break;
    }
    case api::Request::kStats: {
      internal::Request stats_request;

      auto level = request.stats().level();
      stats_request.mutable_stats()->set_id(txn_id);
      stats_request.mutable_stats()->set_level(level);

      // Send to appropriate module based on provided information
      switch (request.stats().module()) {
        case api::StatsModule::SERVER:
          ProcessStatsRequest(stats_request.stats());
          break;
        case api::StatsModule::SCHEDULER:
          SendSameMachine(stats_request, SCHEDULER_CHANNEL);
          break;
        default:
          LOG(ERROR) << "Invalid module for stats request";
          break;
      }
      break;
    }
    default:
      pending_responses_.erase(txn_id);
      LOG(ERROR) << "Unexpected request type received: \""
                 << CASE_NAME(request.type_case(), api::Request) << "\"";
      break;
  }
}

/***********************************************
              Internal Requests
***********************************************/

void Server::HandleInternalRequest(
    internal::Request&& req,
    string&& from_machine_id,
    string&& from_channel) {
  switch (req.type_case()) {
    case internal::Request::kLookupMaster: 
      ProcessLookUpMasterRequest(
          req.mutable_lookup_master(),
          move(from_machine_id),
          move(from_channel));
      break;
    case internal::Request::kCompletedSubtxn:
      ProcessCompletedSubtxn(req.mutable_completed_subtxn());
      break;
    default:
      LOG(ERROR) << "Unexpected request type received: \""
                 << CASE_NAME(req.type_case(), internal::Request) << "\"";
      break;
  }
}

void Server::ProcessLookUpMasterRequest(
    internal::LookupMasterRequest* lookup_master,
    string&& from_machine_id,
    string&& from_channel) {
  internal::Response response;
  auto lookup_response = response.mutable_lookup_master();
  lookup_response->set_txn_id(lookup_master->txn_id());
  auto metadata_map = lookup_response->mutable_master_metadata();
  auto new_keys = lookup_response->mutable_new_keys();
  while (!lookup_master->keys().empty()) {
    auto key = lookup_master->mutable_keys()->ReleaseLast();

    if (!config_->KeyIsInLocalPartition(*key)) {
      // Ignore keys that the current partition does not have
      delete key;
    } else {
      Metadata metadata;
      if (lookup_master_index_->GetMasterMetadata(*key, metadata)) {
        // If key exists, add the metadata of current key to the response
        auto& response_metadata = (*metadata_map)[*key];
        response_metadata.set_master(metadata.master);
        response_metadata.set_counter(metadata.counter);
        delete key;
      } else {
        // Otherwise, add it to the list indicating this is a new key
        new_keys->AddAllocated(key);
      }
    }
  }
  Send(response, from_machine_id, from_channel);
}

void Server::ProcessCompletedSubtxn(internal::CompletedSubtransaction* completed_subtxn) {
  auto txn_id = completed_subtxn->txn().internal().id();
  if (pending_responses_.count(txn_id) == 0) {
    return;
  }
  auto& finished_txn = completed_txns_[txn_id];
  auto sub_txn_origin = completed_subtxn->partition();
  // If this is the first sub-transaction, initialize the
  // finished transaction with this sub-transaction as the starting
  // point and populate the list of partitions that we are still
  // waiting for sub-transactions from. Otherwise, remove the
  // partition of this sub-transaction from the awaiting list and
  // merge the sub-txn to the current txn.
  if (!finished_txn.initialized) {
    finished_txn.txn = completed_subtxn->release_txn();
    finished_txn.awaited_partitions.clear();
    for (auto p : completed_subtxn->involved_partitions()) {
      if (p != sub_txn_origin) {
        finished_txn.awaited_partitions.insert(p);
      }
    }
    finished_txn.initialized = true;
  } else if (finished_txn.awaited_partitions.erase(sub_txn_origin)) {
    MergeTransaction(*finished_txn.txn, completed_subtxn->txn());
  }

  // If all sub-txns are received, response back to the client and
  // clean up all tracking data for this txn.
  if (finished_txn.awaited_partitions.empty()) {
    api::Response response;
    auto txn_response = response.mutable_txn();
    txn_response->set_allocated_txn(finished_txn.txn);
    SendAPIResponse(txn_id, move(response));

    completed_txns_.erase(txn_id);
  }
}

void Server::ProcessStatsRequest(const internal::StatsRequest& stats_request) {
  using rapidjson::StringRef;

  int level = stats_request.level();

  rapidjson::Document stats;
  stats.SetObject();
  auto& alloc = stats.GetAllocator();

  // Add stats for current transactions in the system
  stats.AddMember(StringRef(TXN_ID_COUNTER), txn_id_counter_, alloc);
  stats.AddMember(StringRef(NUM_PENDING_RESPONSES), pending_responses_.size(), alloc);
  stats.AddMember(StringRef(NUM_PARTIALLY_COMPLETED_TXNS), completed_txns_.size(), alloc);
  if (level >= 1) {
    stats.AddMember(
        StringRef(PENDING_RESPONSES),
        ToJsonArrayOfKeyValue(
            pending_responses_,
            [](const auto& resp) { return resp.stream_id; },
            alloc),
        alloc);

    stats.AddMember(
        StringRef(PARTIALLY_COMPLETED_TXNS),
        ToJsonArray(
            completed_txns_,
            [](const auto& p) { return p.first; },
            alloc),
        alloc);
  }

  rapidjson::StringBuffer buf;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
  stats.Accept(writer);

  internal::Response res;
  res.mutable_stats()->set_id(stats_request.id());
  res.mutable_stats()->set_stats_json(buf.GetString());
  HandleInternalResponse(std::move(res));
}


/***********************************************
              Internal Responses
***********************************************/

void Server::HandleInternalResponse(internal::Response&& res) {
  if (res.type_case() != internal::Response::kStats) {
    LOG(ERROR) << "Unexpected response type received: \""
               << CASE_NAME(res.type_case(), internal::Response) << "\"";
    return;
  }
  api::Response response;
  auto stats_response = response.mutable_stats();
  stats_response->set_allocated_stats_json(
      res.mutable_stats()->release_stats_json());
  SendAPIResponse(res.stats().id(), std::move(response));
}

/***********************************************
                    Helpers
***********************************************/

void Server::SendAPIResponse(TxnId txn_id, api::Response&& res) {
  auto& pr = pending_responses_.at(txn_id);
  res.set_stream_id(pr.stream_id);
  pr.response.Set(MM_PROTO, res);
  pr.response.SendTo(client_socket_);
  pending_responses_.erase(txn_id);
}

TxnId Server::NextTxnId() {
  txn_id_counter_++;
  return txn_id_counter_ * MAX_NUM_MACHINES + config_->GetLocalMachineIdAsNumber();
}

} // namespace slog