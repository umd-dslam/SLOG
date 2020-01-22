#include "module/server.h"

#include "common/constants.h"
#include "common/proto_utils.h"
#include "proto/api.pb.h"
#include "proto/internal.pb.h"

using std::move;

namespace slog {

Server::Server(
    shared_ptr<const Configuration> config,
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
    0 /* revent */
  });
}

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
    internal::Request req;
    if (msg.GetProto(req)) {
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
    pending_responses_[txn_id].response = msg;

    internal::Request forward_request;
    forward_request.mutable_forward_txn()->set_allocated_txn(txn);
    SendSameMachine(forward_request, FORWARDER_CHANNEL);
  }
}

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
    case internal::Request::kForwardSubTxn:
      ProcessForwardSubtxnRequest(
          req.mutable_forward_sub_txn(),
          move(from_machine_id));
      break;
    default:
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
      delete key;
    } else {
      Metadata metadata;
      if (lookup_master_index_->GetMasterMetadata(*key, metadata)) {
        auto& response_metadata = (*metadata_map)[*key];
        response_metadata.set_master(metadata.master);
        response_metadata.set_counter(metadata.counter);
        delete key;
      } else {
        new_keys->AddAllocated(key);
      }
    }
  }
  Send(response, from_machine_id, from_channel);
}

void Server::ProcessForwardSubtxnRequest(
    internal::ForwardSubtransaction* forward_sub_txn,
    string&& from_machine_id) {
  auto txn = forward_sub_txn->release_txn();
  auto txn_id = txn->internal().id();
  if (pending_responses_.count(txn_id) == 0) {
    LOG(ERROR) << "Got sub-txn from [" << from_machine_id 
               << "] but there is no pending response for transaction " << txn_id;
    return;
  }
  auto& pr = pending_responses_.at(txn_id);
  if (!pr.initialized) {
    pr.txn = new Transaction();
    pr.awaited_partitions.clear();
    for (auto p :forward_sub_txn->involved_partitions()) {
      pr.awaited_partitions.insert(p);
    }
    pr.initialized = true;
  }

  if (pr.awaited_partitions.erase(forward_sub_txn->partition())) {
    pr.txn->MergeFrom(*txn);
    if (pr.awaited_partitions.empty()) {
      SendAPIResponse(txn_id);
    }
  }
}

void Server::SendAPIResponse(TxnId txn_id) {
  auto& pr = pending_responses_.at(txn_id);
  api::Response response;
  auto txn_response = response.mutable_txn();
  txn_response->set_allocated_txn(pr.txn);
  pr.response.Set(MM_PROTO, response);
  pr.response.SendTo(client_socket_);

  pending_responses_.erase(txn_id);
}


TxnId Server::NextTxnId() {
  txn_id_counter_++;
  return txn_id_counter_ * MAX_NUM_MACHINES + config_->GetLocalMachineIdAsNumber();
}

} // namespace slog