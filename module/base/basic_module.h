#pragma once

#include <vector>

#include "common/constants.h"
#include "common/types.h"
#include "module/base/module.h"
#include "connection/channel.h"
#include "proto/internal.pb.h"

namespace slog {

/**
 * Base class for modules that only need to connect to a single
 * channel for sending and receiving internal messages.
 */
class BasicModule : public Module, public ChannelHolder {
public:
  BasicModule(
      const std::string& name,
      std::unique_ptr<Channel>&& listener);

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

private:
  void SetUp() final;
  void Loop() final;

  std::string name_;
  std::vector<zmq::pollitem_t> poll_items_;
  std::vector<zmq::socket_t> custom_sockets_;
};

} // namespace slog