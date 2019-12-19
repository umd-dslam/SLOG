#pragma once

#include <zmq.hpp>

namespace slog {

class Channel {
public:
  Channel(zmq::context_t& context, const std::string& name);

  zmq::pollitem_t GetPollItem();

  void Receive(zmq::message_t&& msg);

private:
  zmq::socket_t internal_socket_;
  std::string name_;
  std::vector<std::string> whitelisted_msg_;
};

} // namespace slog