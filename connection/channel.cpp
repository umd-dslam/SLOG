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
  socket_.setsockopt(ZMQ_LINGER, 0);
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

void Channel::Send(const MMessage& msg, bool has_more) {
  msg.SendTo(socket_);
  zmq::message_t has_more_msg(1);
  *has_more_msg.data<bool>() = has_more;
  socket_.send(has_more_msg, zmq::send_flags::none);
}

bool Channel::Receive(MMessage& msg) {
  msg.ReceiveFrom(socket_);
  zmq::message_t has_more_msg;
  socket_.recv(has_more_msg);
  return *has_more_msg.data<bool>();
}

std::unique_ptr<Channel> Channel::GetListener() {
  CHECK(!is_listener_) << "Cannot create a listener from a listener";
  CHECK(!listener_created_) << "Listener of channel \"" << name_ 
                            << "\" has already been created";
  listener_created_ = true;
  return std::unique_ptr<Channel>(new Channel(context_, name_, true /* is_listener */));
}

} // namespace slog;