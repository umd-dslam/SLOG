#pragma once

#include <vector>

#include <zmq.hpp>

#include "common/constants.h"
#include "common/mmessage.h"
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
      const std::string& name);

protected:
  virtual std::vector<zmq::socket_t> InitializeCustomSockets();

  virtual void HandleInternalRequest(
      internal::Request&& req,
      std::string&& from_machine_id) = 0;

  virtual void HandleInternalResponse(
      internal::Response&& /* res */,
      std::string&& /* from_machine_id */) {};

  virtual void HandleCustomSocketMessage(
      const MMessage& /* msg */,
      size_t /* socket_index */) {};

  zmq::socket_t& GetCustomSocket(size_t i);

  void Send(
      const google::protobuf::Message& request_or_response,
      const std::string& to_channel,
      const std::string& to_machine_id);

  void Send(
      const google::protobuf::Message& request_or_response,
      const std::string& to_channel);

  const std::shared_ptr<zmq::context_t> GetContext() const;

  const std::string& GetName() const;

private:
  void SetUp() final;
  void Loop() final;

  std::shared_ptr<zmq::context_t> context_;
  std::string name_;
  zmq::socket_t pull_socket_;
  std::vector<zmq::pollitem_t> poll_items_;
  std::vector<zmq::socket_t> custom_sockets_;

  Sender sender_;
};

} // namespace slog