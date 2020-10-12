#include "sender.h"

#include "connection/zmq_utils.h"

using std::move;

namespace slog {

// Must start from 1 because identities starting with 0 are reserved for ZMQ
std::atomic<uint8_t> Sender::counter(1);

Sender::Sender(const std::shared_ptr<Broker>& broker)
  : context_(broker->context()),
    broker_(broker),
    local_machine_id_(broker->GetLocalMachineId()) {}

void Sender::Send(
    const google::protobuf::Message& request_or_response,
    Channel to_channel,
    MachineId to_machine_id) {
  // If sending to local module, use the other function to bypass the local broker
  if (to_machine_id == local_machine_id_) {
    Send(request_or_response, to_channel);
    return;
  }
  // Lazily establish a new connection when necessary
  if (machine_id_to_socket_.count(to_machine_id) == 0) {
    if (auto br = broker_.lock()) {
      zmq::socket_t new_socket(*context_, ZMQ_PUSH);
      new_socket.setsockopt(ZMQ_LINGER, 0);
      new_socket.setsockopt(ZMQ_SNDHWM, 0);
      new_socket.connect(br->GetEndpointByMachineId(to_machine_id));
      machine_id_to_socket_[to_machine_id] = move(new_socket);
    } else {
      // Broker has been destroyed. This can only happen during cleaning up
      return;
    }
  }
  
  SendProto(
      machine_id_to_socket_[to_machine_id],
      request_or_response,
      to_channel,
      local_machine_id_);
}

void Sender::Send(
    const google::protobuf::Message& request_or_response,
    Channel to_channel) {
  // Lazily establish a new connection when necessary
  if (local_channel_to_socket_.count(to_channel) == 0) {
    if (auto br = broker_.lock()) {
      zmq::socket_t new_socket(*context_, ZMQ_PUSH);
      new_socket.connect("inproc://channel_" + std::to_string(to_channel));
      new_socket.setsockopt(ZMQ_LINGER, 0);
      new_socket.setsockopt(ZMQ_SNDHWM, 0);
      local_channel_to_socket_[to_channel] = move(new_socket);
    } else {
      // Broker has been destroyed. This can only happen during cleaning up
      return;
    }
  }

  SendProto(
      local_channel_to_socket_[to_channel],
      request_or_response,
      to_channel,
      local_machine_id_);
}

} // namespace slog