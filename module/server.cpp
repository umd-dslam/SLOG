#include "module/server.h"

#include "common/constants.h"
#include "common/proto_utils.h"
#include "proto/api.pb.h"
#include "proto/internal.pb.h"

namespace slog {

Server::Server(
    shared_ptr<const Configuration> config,
    shared_ptr<zmq::context_t> context,
    Broker& broker,
    shared_ptr<LookupMasterIndex<Key, Metadata>> lookup_master_index)
  : config_(config),
    socket_(*context, ZMQ_ROUTER),
    listener_(broker.AddChannel(SERVER_CHANNEL)),
    lookup_master_index_(lookup_master_index),
    counter_(0) {
  poll_items_.push_back(listener_->GetPollItem());
  poll_items_.push_back({ 
    static_cast<void*>(socket_),
    0, /* fd */
    ZMQ_POLLIN,
    0 /* revent */
  });
}

void Server::SetUp() {
  string endpoint = 
      "tcp://*:" + std::to_string(config_->GetServerPort());
  socket_.bind(endpoint);
}

void Server::Loop() {
  MMessage msg;
  switch (zmq::poll(poll_items_, SERVER_POLL_TIMEOUT_MS)) {
    case 0: // Timed out. No event signaled during poll
      break;
    default:
      if (poll_items_[0].revents & ZMQ_POLLIN) {
        listener_->Receive(msg);
        if (msg.IsProto<internal::Request>()) {
          HandleInternalRequest(std::move(msg));
        } else if (msg.IsProto<internal::Response>()) {
          HandleInternalResponse(std::move(msg));
        }
      }
      if (poll_items_[1].revents & ZMQ_POLLIN) {
        msg.ReceiveFrom(socket_);
        if (msg.IsProto<api::Request>()) {
          HandleAPIRequest(std::move(msg));
        }
      }
      break;
  }

  while (!response_time_.empty()) {
    auto top = response_time_.begin();
    if (top->first <= Clock::now()) {
      auto txn_id = top->second;
      auto& response = pending_response_[txn_id];

      response.SendTo(socket_);

      pending_response_.erase(txn_id);
      response_time_.erase(top);
    } else {
      break;
    }
  }
}

void Server::HandleAPIRequest(MMessage&& msg) {
  api::Request request;
  api::Response response;
  GetRequestAndPrepareResponse(request, response, msg);
  
  if (request.type_case() == api::Request::kTxn) {
    auto txn_response = response.mutable_txn();
    auto txn = txn_response->mutable_txn();
    auto txn_id = NextTxnId();
    txn->CopyFrom(request.txn().txn());
    txn->set_id(txn_id);

    msg.Set(0, response);
    pending_response_[txn_id] = msg;

    response_time_.emplace(
        Clock::now() + 100ms, txn_id);
  }
}

void Server::HandleInternalRequest(MMessage&& msg) {
  internal::Request request;
  internal::Response response;
  GetRequestAndPrepareResponse(request, response, msg);

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

    msg.Set(0, response);
    listener_->Send(msg);
  }
}

void Server::HandleInternalResponse(MMessage&&) {
}

uint32_t Server::NextTxnId() {
  counter_ = (counter_ + 1) % MAX_TXN_COUNT;
  return config_->GetLocalNumericId() * MAX_TXN_COUNT + counter_;
}

} // namespace slog