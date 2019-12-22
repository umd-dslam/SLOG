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
  if (running_) {
    throw std::runtime_error("The module has already started");
  }
  running_ = true;
  return std::thread(&Module::Run, this);
}

void Module::Start() {
  if (running_) {
    return;
  }
  running_ = true;
  Run();
}

void Module::Run() {
  MMessage message;
  while (running_) {
    if (listener_->PollMessage(message, MODULE_POLL_TIMEOUT_MS)) {
      HandleMessage(message);
    }

    PostProcessing();
  }
}

void Module::Send(const MMessage& message) {
  listener_->SendMessage(message);
}

} // namespace slog