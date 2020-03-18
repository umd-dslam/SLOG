#include "module/server.h"

#include "common/constants.h"
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
    }
  }

  if (HasMessageFromClient()) {
    MMessage msg(client_socket_);
    if (msg.IsProto<api::Request>()) {
      HandleAPIRequest(move(msg));
    }
  }
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
  
  if (request.type_case() == api::Request::kTxn) {
    auto txn_id = NextTxnId();
    auto txn = request.mutable_txn()->release_txn();
    auto txn_internal = txn->mutable_internal();
    txn_internal->set_id(txn_id);
    txn_internal
        ->mutable_coordinating_server()
        ->CopyFrom(
            config_->GetLocalMachineIdAsProto());

    CHECK(pending_responses_.count(txn_id) == 0) << "Duplicate transaction id: " << txn_id;
    // The message object is holding the address of the requesting client so we keep it
    // for later use.
    pending_responses_[txn_id].response = msg;
    pending_responses_[txn_id].stream_id = request.stream_id();

    internal::Request forward_request;
    forward_request.mutable_forward_txn()->set_allocated_txn(txn);
    SendSameMachine(forward_request, FORWARDER_CHANNEL);
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
    SendAPIResponse(txn_id, std::move(response));

    completed_txns_.erase(txn_id);
  }
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