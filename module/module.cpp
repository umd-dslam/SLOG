#include "module/module.h"

namespace slog {

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

ModuleRunner::ModuleRunner(Module* module) 
  : module_(module),
    running_(false) {}

ModuleRunner::~ModuleRunner() {
  running_ = false;
  thread_.join();
}

void ModuleRunner::StartInNewThread() {
  if (running_) {
    throw std::runtime_error("The module has already started");
  }
  running_ = true;
  thread_ = std::thread(&ModuleRunner::Run, this);
}

void ModuleRunner::Start() {
  if (running_) {
    throw std::runtime_error("The module has already started");
  }
  running_ = true;
  Run();
}


void ModuleRunner::Run() {
  module_->SetUp();
  while (running_) {
    module_->Loop();
  }
}

} // namespace slog