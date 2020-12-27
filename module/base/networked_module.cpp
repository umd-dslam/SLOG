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

NetworkedModule::NetworkedModule(const std::string& name, const std::shared_ptr<Broker>& broker, Channel channel,
                                 std::chrono::milliseconds poll_timeout_ms, int recv_batch, size_t request_pool_size,
                                 size_t response_pool_size)
    : Module(name),
      context_(broker->context()),
      pull_socket_(*context_, ZMQ_PULL),
      sender_(broker),
      channel_(channel),
      poll_timeout_ms_(poll_timeout_ms),
      recv_batch_(recv_batch),
      request_pool_(request_pool_size),
      response_pool_(response_pool_size) {
  broker->AddChannel(channel);
  pull_socket_.bind("inproc://channel_" + std::to_string(channel));
  pull_socket_.set(zmq::sockopt::linger, 0);
  // Remove limit on the zmq message queues
  pull_socket_.set(zmq::sockopt::rcvhwm, 0);

  poll_items_.push_back({
      static_cast<void*>(pull_socket_), 0, /* fd */
      ZMQ_POLLIN, 0                        /* revent */
  });
}

zmq::socket_t& NetworkedModule::GetCustomSocket(size_t i) { return custom_sockets_[i]; }

const std::shared_ptr<zmq::context_t> NetworkedModule::context() const { return context_; }

void NetworkedModule::SetUp() {
  custom_sockets_ = InitializeCustomSockets();
  for (auto& socket : custom_sockets_) {
    poll_items_.push_back({
        static_cast<void*>(socket), 0, /* fd */
        ZMQ_POLLIN, 0                  /* revent */
    });
  }

  Initialize();
}

void NetworkedModule::Loop() {
  if (!zmq::poll(poll_items_, poll_timeout_ms_)) {
    return;
  }

  for (int i = 0; i < recv_batch_; i++) {
    // Message from pull socket
    if (zmq::message_t msg; pull_socket_.recv(msg, zmq::recv_flags::dontwait)) {
      if (google::protobuf::Any any; ParseAny(any, msg)) {
        if (MachineId from; ParseMachineId(from, msg)) {
          if (any.Is<Request>()) {
            auto req = NewRequest();
            any.UnpackTo(req.get());
            HandleInternalRequest(move(req), from);
          } else if (any.Is<Response>()) {
            auto res = NewResponse();
            any.UnpackTo(res.get());
            HandleInternalResponse(move(res), from);
          }
        }
      }
    }

    // Message from one of the custom sockets. These sockets
    // are indexed from 1 in poll_items_. The first poll item
    // belongs to the channel socket.
    for (size_t i = 1; i < poll_items_.size(); i++) {
      HandleCustomSocket(custom_sockets_[i - 1], i - 1);
    }
  }
}

void NetworkedModule::Send(const google::protobuf::Message& request_or_response, Channel to_channel,
                           MachineId to_machine_id) {
  sender_.Send(request_or_response, to_channel, to_machine_id);
}

void NetworkedModule::Send(const google::protobuf::Message& request_or_response, Channel to_channel) {
  sender_.Send(request_or_response, to_channel);
}

}  // namespace slog