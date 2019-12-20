#pragma once

#include <vector>

#include <google/protobuf/message.h>
#include <zmq.hpp>

#include "proto/internal.pb.h"

using std::string;

namespace slog {

/**
 * Encapsulates a multi-part zmq message. 
 * [identity of connection][empty frame][is response or not][body]
 */
class MMessage {
public:
  MMessage() = default;
  MMessage(const proto::Request& request);
  MMessage(const proto::Response& response);
  MMessage(zmq::socket_t& socket);

  void SetIdentity(std::string&& identity);
  const string& GetIdentity() const;

  void FromRequest(const proto::Request& request);
  bool ToRequest(proto::Request& request) const;

  void FromResponse(const proto::Response& response);
  bool ToResponse(proto::Response& response) const;

  void Send(zmq::socket_t& socket) const;
  void Receive(zmq::socket_t& socket);

  void Clear();

private:
  string identity_;
  bool is_response_;
  string body_;
};

} // namespace slog