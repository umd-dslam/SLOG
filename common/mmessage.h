#pragma once

#include <vector>
#include <unordered_map>

#include <glog/logging.h>
#include <google/protobuf/any.pb.h>
#include <google/protobuf/message.h>
#include <zmq.hpp>

using std::string;
using std::vector;
using std::unordered_map;
using google::protobuf::Any;
using google::protobuf::Message;

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

  void Add(const Message& data);
  void Add(const string& data);
  void Add(string&& data);

  void Set(size_t index, const Message& data);
  void Set(size_t index, const string& data);
  void Set(size_t index, string&& data);

  template<typename T>
  bool GetProto(T& out, size_t index = 0) const {
    if (IsProto<T>(index)) {
      const auto any = GetAny(index);
      return (*any).UnpackTo(&out);
    }
    return false;
  }

  template<typename T>
  bool IsProto(size_t index = 0) const {
    CHECK(index < body_.size()) << "Index out of bound";
    const auto any = GetAny(index);
    return any != nullptr && (*any).Is<T>();
  }

  bool GetString(string& out, size_t index = 0) const;

  void Send(zmq::socket_t& socket) const;
  void Receive(zmq::socket_t& socket);

  void Clear();

private:
  const Any* GetAny(size_t index) const;

  string identity_;
  vector<string> body_;

  mutable unordered_map<size_t, Any> body_to_any_cache_;
};

} // namespace slog