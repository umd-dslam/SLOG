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
  MMessage();
  MMessage(zmq::socket_t& socket);

  size_t Size() const;

  /**
   * Sets the identity portion of a mmessage
   * @param identity Identity to set
   */
  void SetIdentity(const string& identity);
  void SetIdentity(string&& identity);

  /**
   * Gets the identity of a mmessage.
   * @return The identity of current mmessage
   */
  const string& GetIdentity() const;

  /**
   * Checks if the identity portion of a mmessage is empty.
   * @return true if the identity is not empty
   */
  bool HasIdentity() const;

  /**
   * Pushes a protobuf Message, which will be serialized into a 
   * string to the end of this mmessage.
   * @param data a protobuf Message to push
   */
  void Push(const Message& data);

  /**
   * Pushes a string to the end this mmessage
   * @param data the string to push
   */
  void Push(const string& data);
  void Push(string&& data);

  /**
   * Pops a string from the end of this mmessage
   */
  string Pop();

  /**
   * Sets a protobuf Message, which will be serialized into a string
   * at an index of this mmessage. If the index is out of the length
   * of this mmessage, the mmessage is appended with empty strings 
   * it is long enough.
   * 
   * @param index the index to set the data at
   * @param data the protobuf Message to set
   */
  void Set(size_t index, const Message& data);
  /**
   * Sets a string at an index of this mmessage. If the
   * index is out of the length of this mmessage, the mmessage is
   * appended with empty strings it is long enough.
   * 
   * @param index the index to set the data at
   * @param data the string to set
   */
  void Set(size_t index, const string& data);
  void Set(size_t index, string&& data);

  /**
   * Deserializes and gets a protobuf message at a given index.
   * @param out   where the deserialized protobuf message is stored
   * @param index the index of the message in this mmessage to get
   * @return true if the given index has a protobuf message and
   *         false otherwise.
   */
  template<typename T>
  bool GetProto(T& out, size_t index = MM_DATA) const {
    if (IsProto<T>(index)) {
      const auto any = GetAny(index);
      return (*any).UnpackTo(&out);
    }
    return false;
  }

  /**
   * Checks whether the given index has a protobuf message.
   * @param index the index to check
   * @return true if the data at the given index is a protobuf message
   *         and false otherwise.
   */
  template<typename T>
  bool IsProto(size_t index = MM_DATA) const {
    CHECK(index < body_.size()) 
        << "Index out of bound. Size: " << body_.size() << ". Index: " << index;
    const auto any = GetAny(index);
    return any != nullptr && (*any).Is<T>();
  }

  /**
   * Gets a string at a given index
   * @param out   where the resulting string is stored
   * @param index the index of the mmessage to get a string from
   */
  void GetString(string& out, size_t index = 0) const;

  /**
   * Sends the current mmessage to a given socket. The data
   * stored in this mmessage will not be cleared after this.
   */
  void SendTo(zmq::socket_t& socket) const;

  /**
   * Populates the current mmessage with data received from
   * a given socket. All previous data in this mmessage will 
   * be overwritten.
   * If dont_wait is true, this function returns true when it receives
   * data from the socket and false otherwise.
   */
  bool ReceiveFrom(zmq::socket_t& socket, bool dont_wait = false);

  /**
   * Clears all data in this mmessage.
   */
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