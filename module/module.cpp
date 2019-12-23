#include "module/module.h"

namespace slog {

namespace {
const long MODULE_POLL_TIMEOUT_MS = 1000;
}

Module::Module() : running_(false) {}

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
  while (running_) {
    Loop();
  }
}

ChanneledModule::ChanneledModule(Channel* listener) 
  : listener_(listener),
    poll_item_(listener->GetPollItem()) {}

void ChanneledModule::Send(const MMessage& message) {
  listener_->Send(message);
}

void ChanneledModule::Loop() {
  MMessage message;
  int rc = zmq::poll(&poll_item_, 1, MODULE_POLL_TIMEOUT_MS);
  if (rc == 0) {
    HandlePollTimedOut();
  } else if (rc > 0) {
    if (poll_item_.revents & ZMQ_POLLIN) {
      listener_->Receive(message);
      HandleMessage(message);
    }
  }
  PostProcessing();
}

} // namespace slog