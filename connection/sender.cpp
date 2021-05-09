#include "sender.h"

using std::move;

namespace slog {

Sender::Sender(const ConfigurationPtr& config, const std::shared_ptr<zmq::context_t>& context)
    : config_(config), context_(context) {}

void Sender::Send(const internal::Envelope& envelope, MachineId to_machine_id, Channel to_channel) {
  auto& socket = GetRemoteSocket(to_machine_id, to_channel);
  SendSerializedProto(*socket, envelope, config_->local_machine_id(), to_channel);
}

void Sender::Send(EnvelopePtr&& envelope, MachineId to_machine_id, Channel to_channel) {
  if (to_machine_id == config_->local_machine_id()) {
    Send(move(envelope), to_channel);
  } else {
    Send(*envelope, to_machine_id, to_channel);
  }
}

void Sender::Send(EnvelopePtr&& envelope, Channel to_channel) {
  // Lazily establish a new connection when necessary
  auto it = local_channel_to_socket_.find(to_channel);
  if (it == local_channel_to_socket_.end()) {
    zmq::socket_t new_socket(*context_, ZMQ_PUSH);
    new_socket.connect(MakeInProcChannelAddress(to_channel));
    new_socket.set(zmq::sockopt::sndhwm, 0);
    auto res = local_channel_to_socket_.insert_or_assign(to_channel, move(new_socket));
    it = res.first;
  }
  envelope->set_from(config_->local_machine_id());
  SendEnvelope(it->second, move(envelope));
}

void Sender::Send(const internal::Envelope& envelope, const std::vector<MachineId>& to_machine_ids,
                  Channel to_channel) {
  auto serialized = SerializeProto(envelope);
  for (auto dest : to_machine_ids) {
    zmq::message_t copied;
    copied.copy(serialized);
    auto& socket = GetRemoteSocket(dest, to_channel);
    SendAddressedBuffer(*socket, move(copied), config_->local_machine_id(), to_channel);
  }
}

void Sender::Send(EnvelopePtr&& envelope, const std::vector<MachineId>& to_machine_ids, Channel to_channel) {
  auto serialized = SerializeProto(*envelope);
  bool send_local = false;
  for (auto dest : to_machine_ids) {
    if (dest == config_->local_machine_id()) {
      send_local = true;
      continue;
    }
    zmq::message_t copied;
    copied.copy(serialized);
    auto& socket = GetRemoteSocket(dest, to_channel);
    SendAddressedBuffer(*socket, move(copied), config_->local_machine_id(), to_channel);
  }
  if (send_local) {
    Send(std::move(envelope), to_channel);
  }
}

Sender::SocketPtr& Sender::GetRemoteSocket(MachineId machine_id, Channel channel) {
  uint32_t port;
  if (channel >= kMaxChannel) {
    port = config_->broker_ports(config_->broker_ports_size() - 1);
  } else {
    switch (channel) {
      case kForwarderChannel:
        port = config_->forwarder_port();
        break;
      case kSequencerChannel:
        port = config_->sequencer_port();
        break;
      default:
        port = config_->broker_ports(0);
    }
  }

  // Lazily establish a new connection when necessary
  uint64_t machine_id_and_port = (static_cast<uint64_t>(machine_id) << 32) | port;
  auto ins = machine_id_and_port_to_sockets_.try_emplace(machine_id_and_port, nullptr);
  auto& socket = ins.first->second;
  if (socket == nullptr) {
    socket = std::make_unique<zmq::socket_t>(*context_, ZMQ_PUSH);
    socket->set(zmq::sockopt::sndhwm, 0);
    auto endpoint = MakeRemoteAddress(config_->protocol(), config_->address(machine_id), port);
    socket->connect(endpoint);
  }
  return socket;
}

}  // namespace slog