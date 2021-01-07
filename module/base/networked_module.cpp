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
                                 std::chrono::milliseconds poll_timeout, int recv_batch, size_t request_pool_size,
                                 size_t response_pool_size)
    : Module(name),
      context_(broker->context()),
      channel_(channel),
      pull_socket_(*context_, ZMQ_PULL),
      sender_(broker),
      poller_(poll_timeout),
      recv_batch_(recv_batch),
      request_pool_(request_pool_size),
      response_pool_(response_pool_size) {
  broker->AddChannel(channel);
  pull_socket_.bind("inproc://channel_" + std::to_string(channel));
  // Remove limit on the zmq message queues
  pull_socket_.set(zmq::sockopt::rcvhwm, 0);
}

zmq::socket_t& NetworkedModule::GetCustomSocket(size_t i) { return custom_sockets_[i]; }

const std::shared_ptr<zmq::context_t> NetworkedModule::context() const { return context_; }

void NetworkedModule::SetUp() {
  poller_.PushSocket(pull_socket_);
  custom_sockets_ = InitializeCustomSockets();
  for (auto& socket : custom_sockets_) {
    poller_.PushSocket(socket);
  }

  Initialize();
}

bool NetworkedModule::Loop() {
  auto poll_res = poller_.Wait();

  for (auto time_ev : poll_res.time_events) {
    HandleTimeEvent(time_ev);
  }

  if (poll_res.num_zmq_events == 0) {
    return false;
  }

  for (int i = 0; i < recv_batch_; i++) {
    // Message from pull socket
    if (zmq::message_t msg; pull_socket_.recv(msg, zmq::recv_flags::dontwait)) {
      if (google::protobuf::Any any; DeserializeAny(any, msg)) {
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

    for (size_t i = 0; i < custom_sockets_.size(); i++) {
      HandleCustomSocket(custom_sockets_[i], i);
    }
  }

  return false;
}

void NetworkedModule::Send(const google::protobuf::Message& request_or_response, Channel to_channel,
                           MachineId to_machine_id) {
  sender_.Send(request_or_response, to_channel, to_machine_id);
}

void NetworkedModule::Send(const google::protobuf::Message& request_or_response, Channel to_channel) {
  sender_.Send(request_or_response, to_channel);
}

void NetworkedModule::NewTimeEvent(microseconds timeout, void* data) { poller_.AddTimeEvent(timeout, data); }

}  // namespace slog