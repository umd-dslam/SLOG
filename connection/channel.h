#pragma once

#include <zmq.hpp>

#include "common/mmessage.h"
#include "common/types.h"

namespace slog {

class ChannelListener;

/**
 * A Channel is the interface between a module (e.g. Server, Sequencer) and the
 * network layer. The Broker use the channels to pass message from the outside
 * into the internal modules. To get the messages, a module would obtain the
 * listener corresponding to a channel and poll messages via the listener.
 */
class Channel {
public:
  Channel(std::shared_ptr<zmq::context_t> context, ChannelName name);
  Channel(const Channel&) = delete;
  Channel& operator=(const Channel&) = delete;

  /**
   * Returns the name of current channel
   */
  ChannelName GetName() const;

  /**
   * Returns a zmq pollitem data structure for this channel. This method is
   * used by the Broker
   */
  zmq::pollitem_t GetPollItem();

  /**
   * Passes a message to this channel. That message would be received by the
   * listener
   */
  void SendToListener(const MMessage& msg);

  /**
   * Receives a message from the listener
   */
  void ReceiveFromListener(MMessage& msg);

  /**
   * Returns a pointer to the listener corresponding to this channel.
   * This methods should be called on a different thread other than the 
   * Broker thread. Whoever owns this pointer later needs to free it up 
   * otherwise the socket inside the listener would prevent a thread to exit.
   */
  ChannelListener* GetListener();

private:
  std::shared_ptr<zmq::context_t> context_;
  ChannelName name_;
  zmq::socket_t socket_;
  std::atomic<bool> listener_created_;
};

/**
 * A ChannelListener corresponds to a Channel and is used by a module to get and send
 * message from and to a channel
 */
class ChannelListener {
public:
  ChannelListener(std::shared_ptr<zmq::context_t> context, ChannelName name);

  /**
   * Blocks until a message arrived at the channel.
   * Arguments:
   *  msg         - To be filled with the next message in the channel
   *  timeout_ms  - If no message is received by the channel in after timeout_ms 
   *                milliseconds, the method returns with false.
   * Returns true if a message is received, false if timed out.
   */
  bool PollMessage(MMessage& msg, long timeout_ms = -1);

  /**
   * Sends a message to this channel.
   */
  void SendMessage(const MMessage& msg);

private:
  zmq::socket_t socket_;
};

} // namespace slog