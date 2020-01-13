#include "module/base/basic_module.h"

#include "common/constants.h"

using std::move;

namespace slog {

using internal::Request;
using internal::Response;

BasicModule::BasicModule(
    unique_ptr<Channel>&& listener,
    long poll_timeout_ms)
  : ChannelHolder(move(listener)),
    poll_item_(GetChannelPollItem()),
    poll_timeout_ms_(poll_timeout_ms) {}

void BasicModule::Loop() {
  switch (zmq::poll(&poll_item_, 1, poll_timeout_ms_)) {
    case 0: // Timed out. No event signaled during poll
      HandlePollTimedOut();
      break;
    default:
      if (poll_item_.revents & ZMQ_POLLIN) {
        MMessage message;
        ReceiveFromChannel(message);
        auto from_machine_id = message.GetIdentity();

        if (message.IsProto<Request>()) {
          Request req;
          message.GetProto(req);

          string from_channel;
          message.GetString(from_channel, MM_FROM_CHANNEL);

          HandleInternalRequest(
              move(req),
              move(from_machine_id),
              move(from_channel));
        } else if (message.IsProto<Response>()) {
          Response res;
          message.GetProto(res);

          HandleInternalResponse(
              move(res),
              move(from_machine_id));
        }
      }
      break;
  }
  PostProcessing();
}

void BasicModule::SetPollTimeout(long poll_timeout_ms) {
  poll_timeout_ms_ = poll_timeout_ms;
}

} // namespace slog