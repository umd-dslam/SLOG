#pragma once

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
      unique_ptr<Channel>&& listener,
      long wake_up_every_ms = -1L);

protected:
  virtual void HandleInternalRequest(
      internal::Request&& req,
      string&& from_machine_id) = 0;

  virtual void HandleInternalResponse(
      internal::Response&& /* res */,
      string&& /* from_machine_id */) {};

  virtual void HandlePeriodicWakeUp() {};

private:
  void Loop() final;

  bool NeedWakeUp() const;

  zmq::pollitem_t poll_item_;
  long wake_up_every_ms_, poll_timeout_ms_;
  TimePoint wake_up_deadline_;
};

} // namespace slog