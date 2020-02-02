#pragma once

#include <vector>
#include <unordered_map>

#include <glog/logging.h>
#include <google/protobuf/any.pb.h>
#include <google/protobuf/message.h>
#include <zmq.hpp>

#include "common/constants.h"

using std::string;
using std::vector;
using std::unordered_map;
using google::protobuf::Any;
using google::protobuf::Message;

namespace slog {

/**
 * Encapsulates a ZMQ Multipart Message. See
 * 
 * + http://zguide.zeromq.org/php:chapter2#Multipart-Messages
 * + http://zguide.zeromq.org/php:chapter3#The-Request-Reply-Mechanisms
 * 
 * for more about ZMQ Multipart Message. The general structure of
 * a multipart message used here is:
 * 
 * [identity][empty frame][body_0][body_1]...
 *  
 * Where 'identity' stores the identity of the sender and is optional.
 * For example, some messages look like follows:
 *
 * [identity][empty][request][from channel][to channel]
 * [identity][empty][response][empty][to channel]
 */
class MMessage {
public:
  MMessage() = default;
  MMessage(zmq::socket_t& socket);

  size_t Size() const;

  void SetIdentity(const string& identity);
  void SetIdentity(string&& identity);
  const string& GetIdentity() const;
  bool HasIdentity() const;

  void Push(const Message& data);
  void Push(const string& data);
  void Push(string&& data);

  string Pop();

  void Set(size_t index, const Message& data);
  void Set(size_t index, const string& data);
  void Set(size_t index, string&& data);

  template<typename T>
  bool GetProto(T& out, size_t index = MM_PROTO) const {
    if (IsProto<T>(index)) {
      const auto any = GetAny(index);
      return (*any).UnpackTo(&out);
    }
    return false;
  }

  template<typename T>
  bool IsProto(size_t index = MM_PROTO) const {
    CHECK(index < body_.size()) 
        << "Index out of bound. Size: " << body_.size() << ". Index: " << index;
    const auto any = GetAny(index);
    return any != nullptr && (*any).Is<T>();
  }

  bool GetString(string& out, size_t index = 0) const;

  void SendTo(zmq::socket_t& socket) const;
  void ReceiveFrom(zmq::socket_t& socket);

  void Clear();

private:
  void EnsureBodySize(size_t sz);
  const Any* GetAny(size_t index) const;

  string identity_;
  vector<string> body_;

  // A cache to avoid deserializing a proto multiple times
  mutable unordered_map<size_t, Any> body_to_any_cache_;
};

} // namespace slog