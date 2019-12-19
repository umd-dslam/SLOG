#pragma once

#include <zmq.hpp>

#include "common/mmessage.h"

namespace slog {

class ChannelListener;

class Channel {
public:
  Channel(std::shared_ptr<zmq::context_t> context, const std::string& name);
  Channel(const Channel&) = delete;
  Channel& operator=(const Channel&) = delete;

  zmq::pollitem_t GetPollItem();

  void PassToListener(const MMessage& msg);

  ChannelListener* GetListener();

private:
  std::shared_ptr<zmq::context_t> context_;
  std::string name_;
  zmq::socket_t socket_;
};

class ChannelListener {
public:
  ChannelListener(std::shared_ptr<zmq::context_t> context, const std::string& name);

  /**
   * Returns false if timed out.
   */
  bool PollRequest(MMessage& msg, long timeout_us = -1);

  void SendResponse(const MMessage& msg);

private:
  zmq::socket_t socket_;
};

} // namespace slog