#include "module/base/networked_module.h"

#include "common/constants.h"
#include "connection/broker.h"
#include "connection/sender.h"
#include "connection/zmq_utils.h"

using std::move;
using std::unique_ptr;
using std::vector;

namespace slog {

using internal::Request;
using internal::Response;

NetworkedModule::NetworkedModule(
    const std::shared_ptr<Broker>& broker,
    Channel channel)
  : context_(broker->GetContext()),
    pull_socket_(*context_, ZMQ_PULL),
    sender_(broker),
    channel_(channel) {
  broker->AddChannel(channel);
  pull_socket_.bind("inproc://channel_" + std::to_string(channel));
  pull_socket_.setsockopt(ZMQ_LINGER, 0);
  // Remove limit on the zmq message queues
  pull_socket_.setsockopt(ZMQ_RCVHWM, 0);

  poll_items_.push_back({
    static_cast<void*>(pull_socket_),
    0, /* fd */
    ZMQ_POLLIN,
    0 /* revent */
  });
}

zmq::socket_t& NetworkedModule::GetCustomSocket(size_t i) {
  return custom_sockets_[i];
}

const std::shared_ptr<zmq::context_t> NetworkedModule::GetContext() const {
  return context_;
}

void NetworkedModule::SetUp() {
  custom_sockets_ = InitializeCustomSockets();
  for (auto& socket : custom_sockets_) {
    poll_items_.push_back({ 
      static_cast<void*>(socket),
      0, /* fd */
      ZMQ_POLLIN,
      0 /* revent */
    });
  }

  Initialize();
}

void NetworkedModule::Loop() {
  if (!zmq::poll(poll_items_, kModulePollTimeoutMs)) {
    return;
  }

  // Message from pull socket
  if (poll_items_[0].revents & ZMQ_POLLIN) {
    zmq::message_t msg;
    if (pull_socket_.recv(msg) > 0) {
      if (MachineIdNum from; ParseMachineId(from, msg)) {
        if (Request req; ParseProto(req, msg)) {
          HandleInternalRequest(move(req), from);
        } else if (Response res; ParseProto(res, msg)) {
          HandleInternalResponse(move(res), from);
        }
      }
    }
  }

  // Message from one of the custom sockets. These sockets
  // are indexed from 1 in poll_items_. The first poll item
  // belongs to the channel socket.
  for (size_t i = 1; i < poll_items_.size(); i++) {
    if (poll_items_[i].revents & ZMQ_POLLIN) {
      HandleCustomSocket(custom_sockets_[i - 1], i - 1);
    }
  }
}

void NetworkedModule::Send(
    const google::protobuf::Message& request_or_response,
    Channel to_channel,
    MachineIdNum to_machine_id) {
  sender_.Send(request_or_response, to_channel, to_machine_id);
}

void NetworkedModule::Send(
    const google::protobuf::Message& request_or_response,
    Channel to_channel) {
  sender_.Send(request_or_response, to_channel);
}

} // namespace slog