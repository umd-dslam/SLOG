#include "module/module.h"

namespace slog {

namespace {
const long MODULE_POLL_TIMEOUT_MS = 1000;
}

Module::Module(ChannelListener* listener) 
  : listener_(listener),
    running_(false) {}


Module::~Module() {
  running_ = false;
}

std::thread Module::StartInNewThread() {
  running_ = true;
  return std::thread(&Module::Run, this);
}

void Module::Start() {
  running_ = true;
  Run();
}

void Module::Run() {
  MMessage message;
  while (running_) {
    if (listener_->PollMessage(message, MODULE_POLL_TIMEOUT_MS)) {

      if (message.IsResponse()) {

        proto::Response response;
        message.ToResponse(response);

        HandleResponse(response);

      } else {

        proto::Request request;
        message.ToRequest(request);

        HandleRequest(request);

      }
    }

    PostProcessing();
  }
}

void Module::Send(const proto::Request& request) {
  MMessage message(request);
  Send(request);
}

void Module::Send(const proto::Response& response) {

}

void Module::Send(const MMessage& message) {
  listener_->SendMessage(message);
}

} // namespace slog