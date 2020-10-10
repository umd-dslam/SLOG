#pragma once

#include <unordered_map>

#include <zmq.hpp>

#include "common/types.h"
#include "connection/broker.h"

namespace slog {

/*
 * See Broker class for details about this class
 */
class Sender {
public:
  Sender(const std::shared_ptr<Broker>& broker);

  /**
   * Send a request or response to a given channel of a given machine
   * @param request_or_response Request or response to be sent
   * @param to_channel Channel on the machine that this message is sent to
   * @param to_machine_id Id of the machine that this message is sent to
   */
  void Send(
      const google::protobuf::Message& request_or_response,
      Channel to_channel,
      MachineIdNum to_machine_id);

  /**
   * Send a request or response to the same machine given a channel
   * @param request_or_response Request or response to be sent
   * @param to_channel Channel on the machine that this message is sent to
   */
  void Send(
      const google::protobuf::Message& request_or_response,
      Channel to_channel);

private:
  // To make a unique identity for a Sender
  static std::atomic<uint8_t> counter;

  // Keep a pointer to context here to make sure that the below sockets
  // are destroyed before the context is
  std::shared_ptr<zmq::context_t> context_;

  std::weak_ptr<Broker> broker_;

  MachineIdNum local_machine_id_;

  std::unordered_map<MachineIdNum, zmq::socket_t> machine_id_to_socket_;
  std::unordered_map<Channel, zmq::socket_t> local_channel_to_socket_;
};

} // namespace slog