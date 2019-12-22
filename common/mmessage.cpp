#include <glog/logging.h>

#include "common/mmessage.h"

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
  Receive(socket);
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

void MMessage::Add(const MessageLite& data) {
  string buf;
  data.SerializeToString(&buf);
  body_.push_back(std::move(buf));
}

void MMessage::Add(const string& data) {
  body_.push_back(data);
}

void MMessage::Add(string&& data) {
  body_.push_back(std::move(data));
}

void MMessage::Set(size_t index, const MessageLite& data) {
  CHECK(index < body_.size()) << "Index out of bound";
  string buf;
  data.SerializeToString(&buf);
  body_[index] = std::move(buf);
}

void MMessage::Set(size_t index, const string& data) {
  body_[index] = data;
}

void MMessage::Set(size_t index, string&& data) {
  body_[index] = std::move(data);
}

bool MMessage::GetProto(MessageLite& out, size_t index) const {
  CHECK(index < body_.size()) << "Index out of bound";
  return out.ParseFromString(body_[index]);
}

bool MMessage::GetString(string& out, size_t index) const {
  CHECK(index < body_.size()) << "Index out of bound";
  out = body_[index];
  return true;
}

void MMessage::Send(zmq::socket_t& socket) const {
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

void MMessage::Receive(zmq::socket_t& socket) {
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
    body_.push_back(tmp);
  } while (more);
}

void MMessage::Clear() {
  identity_.clear();
  body_.clear();
}

} // namespace slog