#include "module/module.h"

namespace slog {


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
  SetUp();
  while (running_) {
    Loop();
  }
}

ChanneledModule::ChanneledModule(Channel* listener, long poll_timeout_ms) 
  : listener_(listener),
    poll_item_(listener->GetPollItem()),
    poll_timeout_ms_(poll_timeout_ms) {}

void ChanneledModule::Send(const MMessage& message) {
  listener_->Send(message);
}

void ChanneledModule::Loop() {
  MMessage message;
  switch (zmq::poll(&poll_item_, 1, poll_timeout_ms_)) {
    case 0: // Timed out. No event signaled during poll
      HandlePollTimedOut();
      break;
    default:
      if (poll_item_.revents & ZMQ_POLLIN) {
        listener_->Receive(message);
        HandleMessage(message);
      }
      break;
  }
  PostProcessing();
}

} // namespace slog