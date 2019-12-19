#include "connection/channel.h"

namespace slog {

Channel::Channel(
    std::shared_ptr<zmq::context_t> context, 
    const std::string& name) 
  : context_(context),
    name_(name),
    socket_(*context, ZMQ_PAIR) {
  std::string endpoint = "inproc://" + name;
  socket_.connect(endpoint);
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
  return new ChannelListener(context_, name_);
}


ChannelListener::ChannelListener(
    std::shared_ptr<zmq::context_t> context,
    const std::string& name)
  : socket_(*context, ZMQ_PAIR) {
  std::string endpoint = "inproc://" + name;
  socket_.bind(endpoint);
}

bool ChannelListener::PollRequest(MMessage& msg, long timeout_us) {
  zmq::pollitem_t item { 
      static_cast<void*>(socket_), 0, ZMQ_POLLIN, 0 };
  zmq::poll(&item, 1, timeout_us);
  if (item.revents & ZMQ_POLLIN) {
    msg.Receive(socket_);
    return true;
  }
  return false;
}

void ChannelListener::SendResponse(const MMessage& msg) {
  msg.Send(socket_);
}

} // namespace slog;