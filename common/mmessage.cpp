#include "common/mmessage.h"

#include <glog/logging.h>

namespace slog {

namespace {

void SendSingleMessage(
    zmq::socket_t& socket, 
    const string& msg_str,
    bool send_more) {
  zmq::message_t message(msg_str.size());
  memcpy(message.data(), msg_str.data(), msg_str.size());
  socket.send(
      message, 
      send_more ? zmq::send_flags::sndmore : zmq::send_flags::none);
}

// Returns true if there is more
bool ReceiveSingleMessage(
    string& str,
    zmq::socket_t& socket) {
  zmq::message_t message;
  if (!socket.recv(message)) {
    throw std::runtime_error("Malformed multi-part message");
  }
  str = string(
      static_cast<char*>(message.data()),
      message.size());
  return message.more();
}

} // namespace

MMessage::MMessage(zmq::socket_t& socket) {
  ReceiveFrom(socket);
}

size_t MMessage::Size() const {
  return body_.size();
}

void MMessage::SetIdentity(const string& identity) {
  identity_ = identity;
}

void MMessage::SetIdentity(string&& identity) {
  identity_ = std::move(identity);
}

const string& MMessage::GetIdentity() const {
  return identity_;
}

bool MMessage::HasIdentity() const {
  return !identity_.empty();
}

void MMessage::Push(const Message& data) {
  Set(body_.size(), data);
}

void MMessage::Push(const string& data) {
  body_.push_back(data);
  body_to_any_cache_.clear();
}

void MMessage::Push(string&& data) {
  body_.push_back(std::move(data));
  body_to_any_cache_.clear();
}

string MMessage::Pop() {
  string back = body_.back();
  body_.pop_back();
  return back;
}

void MMessage::Set(size_t index, const Message& data) {
  EnsureBodySize(index + 1);
  Any any;
  any.PackFrom(data);
  body_[index] = any.SerializeAsString();
  body_to_any_cache_.clear();
}

void MMessage::Set(size_t index, const string& data) {
  EnsureBodySize(index + 1);
  body_[index] = data;
  body_to_any_cache_.clear();
}

void MMessage::Set(size_t index, string&& data) {
  EnsureBodySize(index + 1);
  body_[index] = std::move(data);
  body_to_any_cache_.clear();
}

bool MMessage::GetString(string& out, size_t index) const {
  CHECK(index < body_.size()) 
      << "Index out of bound. Size: " << body_.size() << ". Index: " << index;
  out = body_[index];
  return true;
}

void MMessage::SendTo(zmq::socket_t& socket) const {
  if (!identity_.empty()) {
    SendSingleMessage(socket, identity_, true);
  }
  SendSingleMessage(socket, "", true);
  size_t remaining = body_.size();
  for (const auto& part : body_) {
    SendSingleMessage(socket, part, remaining - 1 > 0);
    remaining--;
  }
}

void MMessage::ReceiveFrom(zmq::socket_t& socket) {
  Clear();

  string tmp;

  if (!ReceiveSingleMessage(identity_, socket)) {
    return;
  }
  if (!identity_.empty()) {
    // Empty delimiter
    if (!ReceiveSingleMessage(tmp, socket)) {
      return;
    }
  }

  bool more;
  do {
    more = ReceiveSingleMessage(tmp, socket);
    Push(tmp);
  } while (more);
}

void MMessage::Clear() {
  identity_.clear();
  body_.clear();
  body_to_any_cache_.clear();
}

void MMessage::EnsureBodySize(size_t sz) {
  while (body_.size() < sz) {
    body_.push_back("");
  }
}

const Any* MMessage::GetAny(size_t index) const {
  if (body_to_any_cache_.count(index) == 0) {
    auto& any = body_to_any_cache_[index];
    if (!any.ParseFromString(body_[index])) {
      return nullptr;
    }
  }
  return &body_to_any_cache_.at(index);
}

} // namespace slog