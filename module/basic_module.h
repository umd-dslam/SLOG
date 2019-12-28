#pragma once

#include "common/constants.h"
#include "module/module.h"
#include "connection/channel.h"
#include "proto/internal.pb.h"

namespace slog {

/**
 * Base class for modules that only need to connect to a single
 * channel for sending and receiving internal messages.
 */
class BasicModule : public Module, public ChannelHolder {
public:
  BasicModule(Channel* listener, long poll_timeout_ms = BASIC_MODULE_POLL_TIMEOUT_MS);

protected:
  virtual void HandleInternalRequest(
      internal::Request&& req,
      string&& from_machine_id,
      string&& from_channel) = 0;

  virtual void HandleInternalResponse(
      internal::Response&& /* res */,
      string&& /* from_machine_id */) {};

  virtual void HandlePollTimedOut() {};

  virtual void PostProcessing() {};

private:
  void Loop() final;

  zmq::pollitem_t poll_item_;
  long poll_timeout_ms_;
};

} // namespace slog