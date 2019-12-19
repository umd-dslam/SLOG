#include "connection/channel.h"

namespace slog {

Channel::Channel(zmq::context_t& context, const std::string& name) 
  : internal_socket_(context, ZMQ_PAIR),
    name_(name) {
  std::string endpoint = "inproc://" + name;
  internal_socket_.bind(endpoint);
}

zmq::pollitem_t Channel::GetPollItem() {
  return { 
    static_cast<void*>(internal_socket_), 
    0, /* fd */
    ZMQ_POLLIN, 
    0 /* revent */
  };
}

void Channel::Receive(zmq::message_t&& msg) {
  
  internal_socket_.send(msg);
}

} // namespace slog;