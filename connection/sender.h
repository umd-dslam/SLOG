#pragma once

#include <unordered_map>
#include <zmq.hpp>

#include "common/types.h"
#include "connection/broker.h"
#include "connection/zmq_utils.h"
#include "proto/internal.pb.h"

namespace slog {

/*
 * See Broker class for details about this class
 */
class Sender {
 public:
  Sender(const ConfigurationPtr& config, const std::shared_ptr<zmq::context_t>& context);

  /**
   * Send a request or response to a given channel of a given machine
   * @param envelope Request or response to be sent
   * @param to_machine_id Id of the machine that this message is sent to
   * @param to_channel Channel on the machine that this message is sent to
   */
  void Send(const internal::Envelope& envelope, MachineId to_machine_id, Channel to_channel);

  /**
   * Send a request or response to a given channel of a given machine. Use local send
   * if the destination machine is the local machine
   * @param envelope Request or response to be sent
   * @param to_machine_id Id of the machine that this message is sent to
   * @param to_channel Channel on the machine that this message is sent to
   */
  void Send(EnvelopePtr&& envelope, MachineId to_machine_id, Channel to_channel);

  /**
   * Send a request or response to the same machine given a channel
   * @param request_or_response Request or response to be sent
   * @param to_channel Channel on the machine that this message is sent to
   */
  void Send(EnvelopePtr&& envelope, Channel to_channel);

  /**
   * Send a request or response to a given channel of a list of machines.
   * Use this to serialize the message only once.
   * @param request_or_response Request or response to be sent
   * @param to_machine_ids Ids of the machines that this message is sent to
   * @param to_channel Channel on the machine that this message is sent to
   */
  void Send(const internal::Envelope& envelope, const std::vector<MachineId>& to_machine_ids, Channel to_channel);

  /**
   * Send a request or response to a given channel of a list of machines.
   * If local machine is among the destination, local send is used for that machine.
   * Use this to serialize the message only once.
   * @param request_or_response Request or response to be sent
   * @param to_machine_ids Ids of the machines that this message is sent to
   * @param to_channel Channel on the machine that this message is sent to
   */
  void Send(EnvelopePtr&& envelope, const std::vector<MachineId>& to_machine_ids, Channel to_channel);

 private:
  using SocketPtr = std::unique_ptr<zmq::socket_t>;
  SocketPtr& GetRemoteSocket(MachineId machine_id, Channel channel);

  ConfigurationPtr config_;
  // Keep a pointer to context here to make sure that the below sockets
  // are destroyed before the context is
  std::shared_ptr<zmq::context_t> context_;
  std::unordered_map<uint64_t, SocketPtr> machine_id_and_port_to_sockets_;
  std::unordered_map<Channel, zmq::socket_t> local_channel_to_socket_;
};

}  // namespace slog