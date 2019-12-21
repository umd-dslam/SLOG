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
  MMessage(zmq::socket_t& socket);

  void SetIdentity(const string& identity);
  void SetIdentity(string&& identity);
  // This method is coupled with how we define slog id.
  // If this class is to be reused in another project,
  // consider modifying or removing this method
  void SetIdentity(uint32_t replica, uint32_t partition);
  void RemoveIdentity();
  const string& GetIdentity() const;

  void SetChannel(const string& channel);
  void SetChannel(string&& channel);
  const string& GetChannel() const;

  void FromRequest(const proto::Request& request);
  bool ToRequest(proto::Request& request) const;

  void SetResponse(const proto::Response& response);
  bool ToResponse(proto::Response& response) const;

  void Send(zmq::socket_t& socket) const;
  void Receive(zmq::socket_t& socket);

  void Clear();

private:
  string identity_;
  string channel_;
  bool is_response_;
  string body_;

  bool has_identity_;
};

} // namespace slog