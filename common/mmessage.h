#pragma once

#include <vector>

#include <glog/logging.h>
#include <google/protobuf/message.h>
#include <zmq.hpp>

#include "proto/request.pb.h"
#include "proto/response.pb.h"

using std::string;
using std::vector;
using google::protobuf::MessageLite;

namespace slog {

/**
 * Encapsulates a multi-part zmq message:
 * 
 * [source id][empty frame][channel][body]
 * 
 * Source id (called 'identity' in ZMQ) is optional
 */
class MMessage {
public:
  MMessage() = default;
  MMessage(zmq::socket_t& socket);

  void SetIdentity(const string& identity);
  void SetIdentity(string&& identity);
  const string& GetIdentity() const;
  bool HasIdentity() const;

  void Add(const MessageLite& data);
  void Add(const string& data);
  void Add(string&& data);

  void Set(size_t index, const MessageLite& data);
  void Set(size_t index, const string& data);
  void Set(size_t index, string&& data);

  bool GetProto(MessageLite& out, size_t index = 0) const;

  bool GetString(string& out, size_t index = 0) const;

  void Send(zmq::socket_t& socket) const;
  void Receive(zmq::socket_t& socket);

  void Clear();

private:
  string identity_;
  vector<string> body_;
};

} // namespace slog