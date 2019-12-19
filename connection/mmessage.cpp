#include <glog/logging.h>

#include "connection/mmessage.h"

namespace slog {

std::string MessageToString(zmq::message_t& message) {
  return std::string(static_cast<char*>(message.data()), message.size());
}

MMessage::MMessage(const proto::Request& request) {
  FromRequest(request);
}

MMessage::MMessage(const proto::Response& response) {
  FromResponse(response);
}

MMessage::MMessage(zmq::socket_t& socket) {
  Receive(socket);
}

void MMessage::Send(zmq::socket_t& socket) {
  zmq::message_t message;
  // Send the connection identity
  if (!identity_.empty()) {
    message.rebuild(identity_.size());
    memcpy(message.data(), identity_.data(), identity_.size());
    socket.send(message, zmq::send_flags::sndmore);
  }

  // Send the empty delimiter frame
  message.rebuild();
  socket.send(message, zmq::send_flags::sndmore);

  // Send message type
  auto type_str = std::to_string(message_type_);
  message.rebuild(type_str.size());
  memcpy(message.data(), type_str.data(), type_str.size());
  socket.send(message, zmq::send_flags::sndmore);

  // Send the body
  for (size_t i = 0; i < body_.size(); i++) {
    const auto& part = body_[i];
    message.rebuild(part.size());
    memcpy(message.data(), part.data(), part.size());
    auto send_more = i == body_.size() - 1 
        ? zmq::send_flags::none 
        : zmq::send_flags::sndmore;
    socket.send(message, send_more);
  }
}

void MMessage::Receive(zmq::socket_t& socket) {
  Clear();

  zmq::message_t message;
  if (socket.recv(message)) {
    identity_ = MessageToString(message);
  }
  // Read the empty delimiter
  socket.recv(message);
  if (socket.recv(message)) {
    message_type_ = std::stoi(MessageToString(message));
  }
  while (true) {
    if (!socket.recv(message)) {
      break;
    }
    body_.push_back(MessageToString(message));
    if (!message.more()) {
      break;
    }
  }
}

void MMessage::FromRequest(const proto::Request& request) {
  Clear();

  message_type_ = request.type_case();
  
  std::string buf;
  request.SerializeToString(&buf);
  body_.push_back(buf);
}

void MMessage::FromResponse(const proto::Response& response) {
  Clear();

  message_type_ = response.type_case();

  std::string buf;
  response.SerializeToString(&buf);
  body_.push_back(buf);
}

bool MMessage::ToRequest(proto::Request& request) {
  if (body_.empty()) {
    return false;
  }
  return request.ParseFromString(body_[0]);
}

bool MMessage::ToResponse(proto::Response& response) {
  if (body_.empty()) {
    return false;
  }
  return response.ParseFromString(body_[0]);
}

int MMessage::GetType() {
  return message_type_;
}

void MMessage::Clear() {
  identity_.clear();
  message_type_ = 0;
  body_.clear();
}

} // namespace slog