#include "module/base/basic_module.h"

#include "common/constants.h"

using std::move;

namespace slog {

namespace {

long DurationInMs(Duration duration) {
  return duration_cast<milliseconds>(duration).count();
}

} // namespace

using internal::Request;
using internal::Response;

BasicModule::BasicModule(
    const std::string& name,
    unique_ptr<Channel>&& listener,
    long wake_up_every_ms)
  : ChannelHolder(move(listener)),
    name_(name),
    poll_item_(GetChannelPollItem()),
    wake_up_every_ms_(wake_up_every_ms),
    wake_up_deadline_(Clock::now()) {
  if (NeedWakeUp()) {
    poll_timeout_ms_ = wake_up_every_ms;
  } else {
    poll_timeout_ms_ = MODULE_POLL_TIMEOUT_MS;
  }
}

void BasicModule::Loop() {
  if (zmq::poll(&poll_item_, 1, poll_timeout_ms_)) {
    MMessage message;
    ReceiveFromChannel(message);
    auto from_machine_id = message.GetIdentity();

    if (message.IsProto<Request>()) {
      Request req;
      message.GetProto(req);

      HandleInternalRequest(
          move(req),
          move(from_machine_id));

    } else if (message.IsProto<Response>()) {
      Response res;
      message.GetProto(res);

      HandleInternalResponse(
          move(res),
          move(from_machine_id));
    }
  } else {
    if (NeedWakeUp()) {
      HandlePeriodicWakeUp();
    }
  }

  if (NeedWakeUp()) {
    auto now = Clock::now();
    while (now >= wake_up_deadline_) {
      wake_up_deadline_ += milliseconds(wake_up_every_ms_);
    }
    poll_timeout_ms_ = 1 + DurationInMs(wake_up_deadline_ - now);

    VLOG_EVERY_N(5, 5000 / wake_up_every_ms_)
            << "Now: " << DurationInMs(now.time_since_epoch())
            << " Deadline: " << DurationInMs(wake_up_deadline_.time_since_epoch())
            << " Timeout: " << poll_timeout_ms_ << " ms";

    VLOG_EVERY_N(4, 5000 / wake_up_every_ms_) << name_ << " is alive";
  } else {
    VLOG_EVERY_N(4, 5000 / MODULE_POLL_TIMEOUT_MS) << name_ << " is alive";
  }
}

bool BasicModule::NeedWakeUp() const {
  return wake_up_every_ms_ > 0;
}

} // namespace slog