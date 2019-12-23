#include "module/server.h"

#include "common/constants.h"
#include "proto/api.pb.h"
#include "proto/internal.pb.h"

namespace slog {

Server::Server(
    std::shared_ptr<const Configuration> config,
    std::shared_ptr<zmq::context_t> context,
    Broker& broker)
  : config_(config),
    socket_(*context, ZMQ_ROUTER),
    listener_(broker.AddChannel(SERVER_CHANNEL)),
    rand_eng_(std::random_device{}()),
    dist_(100, 5000),
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
  switch (zmq::poll(poll_items_, 1000)) {
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
        msg.Receive(socket_);
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
      
      response.Send(socket_);

      pending_response_.erase(txn_id);
      response_time_.erase(top);
    } else {
      break;
    }
  }
}

void Server::HandleAPIRequest(MMessage&& msg) {
  api::Request request;
  CHECK(msg.GetProto(request));

  api::Response response;
  response.set_stream_id(request.stream_id());

  if (request.type_case() == api::Request::kTxn) {
    auto txn_response = response.mutable_txn();
    auto txn = txn_response->mutable_txn();
    txn->CopyFrom(request.txn().txn());

    auto txn_id = NextTxnId();
    txn->set_id(txn_id);

    msg.Set(0, response);
    pending_response_[txn_id] = msg;

    auto deadline = Clock::now() + milliseconds(dist_(rand_eng_));
    response_time_.emplace(deadline, txn_id);
  }
}

void Server::HandleInternalRequest(MMessage&& msg) {

}

void Server::HandleInternalResponse(MMessage&& msg) {

}

uint32_t Server::NextTxnId() {
  counter_ = (counter_ + 1) % MAX_TXN_COUNT;
  return config_->GetLocalNumericId() * MAX_TXN_COUNT + counter_;
}

} // namespace slog