#pragma once

#include <vector>

#include <zmq.hpp>

#include "common/constants.h"
#include "common/types.h"
#include "connection/broker.h"
#include "connection/sender.h"
#include "module/base/module.h"
#include "proto/internal.pb.h"

namespace slog {

/**
 * Base class for modules that can send and receive in internal messages.
 */
class NetworkedModule : public Module {
public:
  NetworkedModule(
      const std::shared_ptr<Broker>& broker,
      Channel channel);

protected:
  virtual std::vector<zmq::socket_t> InitializeCustomSockets() {
    return {};
  }

  virtual void Initialize() {};

  virtual void HandleInternalRequest(
      internal::Request&& req,
      MachineId from_machine_id) = 0;

  virtual void HandleInternalResponse(
      internal::Response&& /* res */,
      MachineId /* from_machine_id */) {};

  // The implementation of this function must never block
  virtual bool HandleCustomSocket(
      zmq::socket_t& /* socket */,
      size_t /* socket_index */) { return false; };

  zmq::socket_t& GetCustomSocket(size_t i);

  void Send(
      const google::protobuf::Message& request_or_response,
      Channel to_channel,
      MachineId to_machine_id);

  void Send(
      const google::protobuf::Message& request_or_response,
      Channel to_channel);

  const std::shared_ptr<zmq::context_t> context() const;

  Channel channel() const {
    return channel_;
  }

private:
  void SetUp() final;
  void Loop() final;

  std::shared_ptr<zmq::context_t> context_;
  zmq::socket_t pull_socket_;
  std::vector<zmq::pollitem_t> poll_items_;
  std::vector<zmq::socket_t> custom_sockets_;
  Sender sender_;
  Channel channel_;
};

} // namespace slog
