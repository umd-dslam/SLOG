#include "connection/channel.h"

#include <glog/logging.h>

namespace slog {

Channel::Channel(
    std::shared_ptr<zmq::context_t> context, 
    const std::string& name) 
  : Channel(context, name, false) {}

Channel::Channel(
    std::shared_ptr<zmq::context_t> context, 
    const std::string& name, 
    bool is_listener)
  : context_(context),
    name_(name),
    socket_(*context, ZMQ_PAIR),
    is_listener_(is_listener),
    listener_created_(false) {
  string endpoint = "inproc://" + name;
  if (is_listener) {
    socket_.bind(endpoint);
  } else {
    socket_.connect(endpoint);
  }
}

const std::string& Channel::GetName() const {
  return name_;
}

zmq::pollitem_t Channel::GetPollItem() {
  return { 
    static_cast<void*>(socket_),
    0, /* fd */
    ZMQ_POLLIN,
    0 /* revent */
  };
}

void Channel::Send(const MMessage& msg) {
  msg.SendTo(socket_);
}

void Channel::Receive(MMessage& msg) {
  msg.ReceiveFrom(socket_);
}

std::unique_ptr<Channel> Channel::GetListener() {
  CHECK(!is_listener_) << "Cannot create a listener from a listener";
  CHECK(!listener_created_) << "Listener of channel \"" << name_ 
                            << "\" has already been created";
  listener_created_ = true;
  return std::unique_ptr<Channel>(new Channel(context_, name_, true /* is_listener */));
}

} // namespace slog;