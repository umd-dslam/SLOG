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

MMessage::MMessage(zmq::socket_t& socket) {
  Receive(socket);
}

void MMessage::SetIdentity(const string& identity) {
  identity_ = identity;
  has_identity_ = true;
}

void MMessage::SetIdentity(string&& identity) {
  identity_ = std::move(identity);
  has_identity_ = true;
}

void MMessage::SetIdentity(uint32_t replica, uint32_t partition) {
  proto::SlogIdentifier slog_id;
  slog_id.set_replica(replica);
  slog_id.set_partition(partition);
  slog_id.SerializeToString(&identity_);
  has_identity_ = true;
}

void MMessage::RemoveIdentity() {
  identity_ = "";
  has_identity_ = false;
}

const string& MMessage::GetIdentity() const {
  if (!has_identity_) {
    throw std::runtime_error("No identity set");
  }
  return identity_;
}

void MMessage::SetChannel(ChannelName channel) {
  channel_ = std::to_string(static_cast<size_t>(channel));
}

ChannelName MMessage::GetChannel() const {
  return static_cast<ChannelName>(std::stoul(channel_));
}

void MMessage::Send(zmq::socket_t& socket) const {
  if (has_identity_) {
    SendSingleMessage(socket, identity_, true);
  }
  SendSingleMessage(socket, "", true);
  SendSingleMessage(socket, channel_, true);
  SendSingleMessage(socket, std::to_string((int)is_response_), true);
  SendSingleMessage(socket, body_, false);
}

void MMessage::Receive(zmq::socket_t& socket) {
  Clear();

  string dummy;
  bool more;

  has_identity_ = true;
  if (!ReceiveSingleMessage(identity_, socket)) {
    return;
  }
  // Empty delimiter
  if (!ReceiveSingleMessage(dummy, socket)) {
    return;
  }
  
  if (!ReceiveSingleMessage(channel_, socket)) {
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

bool MMessage::ToRequest(proto::Request& request) const {
  if (is_response_ || body_.empty()) {
    return false;
  }
  return request.ParseFromString(body_);
}

void MMessage::SetResponse(const proto::Response& response) {
  is_response_ = true;
  response.SerializeToString(&body_);
}

bool MMessage::ToResponse(proto::Response& response) const {
  if (!is_response_ || body_.empty()) {
    return false;
  }
  return response.ParseFromString(body_);
}

void MMessage::Clear() {
  identity_.clear();
  has_identity_ = false;
  is_response_ = false;
  body_.clear();
}

} // namespace slog