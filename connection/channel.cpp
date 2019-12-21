#include "connection/channel.h"

namespace slog {

namespace {

std::string MakeEndpoint(ChannelName name) {
  return "inproc://channel_" + std::to_string((int)name);
}

} // namespace 

Channel::Channel(
    std::shared_ptr<zmq::context_t> context, 
    ChannelName name) 
  : context_(context),
    name_(name),
    socket_(*context, ZMQ_PAIR),
    listener_created_(false) {
  auto endpoint = MakeEndpoint(name);
  socket_.connect(endpoint);
}

ChannelName Channel::GetName() const {
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

void Channel::SendToListener(const MMessage& msg) {
  msg.Send(socket_);
}

void Channel::ReceiveFromListener(MMessage& msg) {
  msg.Receive(socket_);
}

ChannelListener* Channel::GetListener() {
  if (listener_created_) {
    throw std::runtime_error("Listener has already been created");
  }
  listener_created_ = true;
  return new ChannelListener(context_, name_);
}


ChannelListener::ChannelListener(
    std::shared_ptr<zmq::context_t> context,
    ChannelName name)
  : socket_(*context, ZMQ_PAIR) {
  auto endpoint = MakeEndpoint(name);
  socket_.bind(endpoint);
}

bool ChannelListener::PollMessage(MMessage& msg, long timeout_ms) {
  zmq::pollitem_t item { 
      static_cast<void*>(socket_), 0, ZMQ_POLLIN, 0 };
  zmq::poll(&item, 1, timeout_ms);
  if (item.revents & ZMQ_POLLIN) {
    msg.Receive(socket_);
    return true;
  }
  return false;
}

void ChannelListener::SendMessage(const MMessage& msg) {
  msg.Send(socket_);
}

} // namespace slog;