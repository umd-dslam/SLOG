#pragma once

#include <vector>

#include <google/protobuf/message.h>
#include <zmq.hpp>

#include "proto/internal.pb.h"

namespace slog {

/**
 * Encapsulates a multi-part zmq message. 
 * [identity of connection][empty frame][message type][body]
 */
class MMessage {
public:
  MMessage() = default;
  MMessage(const proto::Request& request);
  MMessage(const proto::Response& response);
  MMessage(zmq::socket_t& socket);

  void FromRequest(const proto::Request& request);
  bool ToRequest(proto::Request& request);

  void FromResponse(const proto::Response& response);
  bool ToResponse(proto::Response& response);

  void Send(zmq::socket_t& socket);
  void Receive(zmq::socket_t& socket);

  int GetType();

  void Clear();

private:
  std::string identity_;
  // Message type is the type case of a Request or Response
  int message_type_;
  std::vector<std::string> body_;
};

} // namespace slog