#include "module/base/basic_module.h"

#include "common/constants.h"

using std::move;
using std::unique_ptr;
using std::vector;

namespace slog {

using internal::Request;
using internal::Response;

BasicModule::BasicModule(
    const std::string& name,
    unique_ptr<Channel>&& listener)
  : ChannelHolder(move(listener)),
    name_(name) {
  poll_items_.push_back(GetChannelPollItem());
}

vector<zmq::socket_t> BasicModule::InitializeCustomSockets() {
  return {};
}

void BasicModule::SetUp() {
  custom_sockets_ = InitializeCustomSockets();
  for (auto& socket : custom_sockets_) {
    poll_items_.push_back({ 
      static_cast<void*>(socket),
      0, /* fd */
      ZMQ_POLLIN,
      0 /* revent */
    });
  }
}

void BasicModule::Loop() {
  if (!zmq::poll(poll_items_, MODULE_POLL_TIMEOUT_MS)) {
    return;
  }

  // Message from channel
  if (poll_items_[0].revents & ZMQ_POLLIN) {
    MMessage message;
    ReceiveFromChannel(message);
    auto from_machine_id = message.GetIdentity();

    if (message.IsProto<Request>()) {
      Request req;
      message.GetProto(req);

      HandleInternalRequest(
          move(req),
          move(from_machine_id));

    } else if (message.IsProto<Response>()) {
      Response res;
      message.GetProto(res);

      HandleInternalResponse(
          move(res),
          move(from_machine_id));
    }
  }

  // Message from one of the custom sockets. These sockets
  // are indexed from 1 in poll_items_. The first poll item
  // belongs to the channel socket.
  for (size_t i = 1; i < poll_items_.size(); i++) {
    if (poll_items_[i].revents & ZMQ_POLLIN) {
      MMessage msg(custom_sockets_[i - 1]);
      HandleCustomSocketMessage(msg, i - 1 /* socket_index */);
    }
  }
}

} // namespace slog