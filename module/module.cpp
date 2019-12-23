#include "module/module.h"

namespace slog {

namespace {
const long MODULE_POLL_TIMEOUT_MS = 1000;
}

Module::Module(Channel* listener) 
  : listener_(listener),
    running_(false) {
  poll_items_.emplace_back(listener_->GetPollItem());
}

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
    int rc = zmq::poll(poll_items_, MODULE_POLL_TIMEOUT_MS);
    if (rc == 0) {
      HandlePollTimedOut();
    } else if (rc > 0) {
      if (poll_items_[0].revents & ZMQ_POLLIN) {
        listener_->Receive(message);
        HandleMessage(message);
      } 
      for (size_t i = 1; i < poll_items_.size(); i++) {
        if (poll_items_[i].revents & ZMQ_POLLIN) {
          auto s = static_cast<zmq::socket_t*>(poll_items_[i].socket);
          message.Receive(*s);
          HandleMessage(message);
        }
      }
    }
    PostProcessing();
  }
}

void Module::AddPollItem(zmq::pollitem_t&& poll_item) {
  poll_items_.push_back(std::move(poll_item));
}

void Module::Send(const MMessage& message) {
  listener_->Send(message);
}

} // namespace slog