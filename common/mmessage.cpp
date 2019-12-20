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

MMessage::MMessage(const proto::Request& request) {
  FromRequest(request);
}

MMessage::MMessage(const proto::Response& response) {
  FromResponse(response);
}

MMessage::MMessage(zmq::socket_t& socket) {
  Receive(socket);
}

void MMessage::SetIdentity(string&& identity) {
  identity_ = std::move(identity);
}

const string& MMessage::GetIdentity() const {
  return identity_;
}

void MMessage::Send(zmq::socket_t& socket) const {
  if (!identity_.empty()) {
    SendSingleMessage(socket, identity_, true);
  }
  SendSingleMessage(socket, "", true);
  SendSingleMessage(socket, std::to_string((int)is_response_), true);
  SendSingleMessage(socket, body_, false);
}

void MMessage::Receive(zmq::socket_t& socket) {
  Clear();

  string dummy;
  bool more;

  if (!ReceiveSingleMessage(identity_, socket)) {
    return;
  }
  // Empty delimiter
  if (!ReceiveSingleMessage(dummy, socket)) {
    return;
  }
  
  more = ReceiveSingleMessage(dummy, socket);
  is_response_ = dummy == "1";
  if (!more) { return; }

  if (!ReceiveSingleMessage(body_, socket)) {
    return;
  }

  // Ignore the rest, if any
  while (ReceiveSingleMessage(dummy, socket)) {}
}

void MMessage::FromRequest(const proto::Request& request) {
  Clear();  
  is_response_ = false;
  request.SerializeToString(&body_);
}

void MMessage::FromResponse(const proto::Response& response) {
  Clear();
  is_response_ = true;
  response.SerializeToString(&body_);
}

bool MMessage::ToRequest(proto::Request& request) const {
  if (is_response_ || body_.empty()) {
    return false;
  }
  return request.ParseFromString(body_);
}

bool MMessage::ToResponse(proto::Response& response) const {
  if (!is_response_ || body_.empty()) {
    return false;
  }
  return response.ParseFromString(body_);
}

void MMessage::Clear() {
  identity_.clear();
  is_response_ = false;
  body_.clear();
}

} // namespace slog