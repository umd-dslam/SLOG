#include "module/server.h"

#include "common/constants.h"
#include "common/proto_utils.h"
#include "proto/api.pb.h"
#include "proto/internal.pb.h"

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
  switch (zmq::poll(poll_items_, SERVER_POLL_TIMEOUT_MS)) {
    case 0: // Timed out. No event signaled during poll
      break;
    default: {
      MMessage msg;

      if (HasMessageFromChannel()) {
        ReceiveFromChannel(msg);
        if (msg.IsProto<internal::Request>()) {
          HandleInternalRequest(std::move(msg));
        } else if (msg.IsProto<internal::Response>()) {
          HandleInternalResponse(std::move(msg));
        }
      }

      if (HasMessageFromClient()) {
        msg.ReceiveFrom(client_socket_);
        if (msg.IsProto<api::Request>()) {
          HandleAPIRequest(std::move(msg));
        }
      }

      break;
    }
  }

  // For testing the server. To be remove later
  while (!response_time_.empty()) {
    auto top = response_time_.begin();
    if (top->first <= Clock::now()) {
      auto txn_id = top->second;
      auto& response = pending_response_[txn_id];

      response.SendTo(client_socket_);

      pending_response_.erase(txn_id);
      response_time_.erase(top);
    } else {
      break;
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
  api::Response response;
  CHECK(msg.GetProto(request));
  response.set_stream_id(request.stream_id());
  
  if (request.type_case() == api::Request::kTxn) {
    // TODO: reject transactions with empty read set and write set
    auto txn_id = NextTxnId();

    pending_response_[txn_id] = msg;

    internal::Request forward_request;
    auto forwarded_txn = forward_request
        .mutable_forward_txn()
        ->mutable_txn();
    forwarded_txn->CopyFrom(request.txn().txn());
    forwarded_txn->mutable_internal()->set_id(txn_id);

    SendSameMachine(forward_request, FORWARDER_CHANNEL);

    // For testing the server. To be remove later
    response_time_.emplace(
        Clock::now() + 100ms, txn_id);
  }
}

void Server::HandleInternalRequest(MMessage&& msg) {
  internal::Request request;
  internal::Response response;
  CHECK(msg.GetProto(request));

  if (request.type_case() == internal::Request::kLookupMaster) {
    auto& lookup_request = request.lookup_master();
    auto lookup_response = response.mutable_lookup_master();
    auto metadata_map = lookup_response->mutable_master_metadata();

    for (const auto& key : lookup_request.keys()) {
      Metadata metadata;
      if (lookup_master_index_->GetMasterMetadata(key, metadata)) {
        auto& response_metadata = (*metadata_map)[key];
        response_metadata.set_master(metadata.master);
        response_metadata.set_counter(metadata.counter);
      }
    }
    lookup_response->set_txn_id(lookup_request.txn_id());
    
    msg.Set(MM_PROTO, response);
    Send(std::move(msg));
  }
}

void Server::HandleInternalResponse(MMessage&&) {
}

TxnId Server::NextTxnId() {
  txn_id_counter_ = (txn_id_counter_ + 1) % MAX_TXN_COUNT;
  return config_->GetLocalMachineIdAsNumber() * MAX_TXN_COUNT + txn_id_counter_;
}

} // namespace slog