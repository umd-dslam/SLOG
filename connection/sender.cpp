#include "sender.h"

using std::move;

namespace slog {

// Must start from 1 because identities starting with 0 are reserved for ZMQ
std::atomic<uint8_t> Sender::counter(1);

Sender::Sender(const std::shared_ptr<Broker>& broker)
  : context_(broker->GetContext()),
    broker_(broker),
    local_machine_id_(broker->GetLocalMachineId()) {
  identity_ = static_cast<char>(counter.fetch_add(1)) + local_machine_id_;
}

void Sender::Send(
    const google::protobuf::Message& request_or_response,
    const std::string& to_channel,
    const std::string& to_machine_id) {
  if (to_machine_id.empty() || to_machine_id == local_machine_id_) {
    Send(request_or_response, to_channel);
    return;
  }
  if (machine_id_to_socket_.count(to_machine_id) == 0) {
    if (auto br = broker_.lock()) {
      zmq::socket_t new_socket(*context_, ZMQ_DEALER);
      // The identity must be set before calling ``connect''
      new_socket.setsockopt(
          ZMQ_IDENTITY, identity_.c_str(), identity_.length());
      new_socket.setsockopt(ZMQ_LINGER, 0);
      new_socket.setsockopt(ZMQ_SNDHWM, 0);
      new_socket.connect(br->GetEndpointByMachineId(to_machine_id));
      machine_id_to_socket_[to_machine_id] = move(new_socket);
    } else {
      // Broker has been destroyed. This can only happen during clean up
      // when the whole process is terminated.
      return;
    }
  }
  MMessage message;
  message.Set(MM_PROTO, request_or_response);
  message.Set(MM_TO_CHANNEL, to_channel);
  message.SendTo(machine_id_to_socket_[to_machine_id]);
}

void Sender::Send(
    const google::protobuf::Message& request_or_response,
    const std::string& to_channel) {
  if (local_channel_to_socket_.count(to_channel) == 0) {
    if (auto br = broker_.lock()) {
      zmq::socket_t new_socket(*context_, ZMQ_PUSH);
      new_socket.connect("inproc://" + to_channel);
      new_socket.setsockopt(ZMQ_LINGER, 0);
      new_socket.setsockopt(ZMQ_SNDHWM, 0);
      local_channel_to_socket_[to_channel] = move(new_socket);
    } else {
      // Broker has been destroyed. This can only happen during clean up
      // when the whole process is terminated.
      return;
    }
  }
  MMessage message;
  message.Set(MM_PROTO, request_or_response);
  message.SendTo(local_channel_to_socket_[to_channel]);
}

} // namespace slog