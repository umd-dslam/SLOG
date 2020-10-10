#pragma once

#include <zmq.hpp>
#include <google/protobuf/any.pb.h>

#include "common/types.h"

namespace slog {

inline void SendProto(
    zmq::socket_t& socket,
    const google::protobuf::Message& proto,
    Channel chan = 0) {
  google::protobuf::Any any;
  any.PackFrom(proto);
  
  size_t sz = sizeof(Channel) + any.ByteSizeLong();
  zmq::message_t msg(sz);

  // The first 4 bytes indicates receiving channel
  memcpy(msg.data(), &chan, sizeof(Channel));
  // The rest is the actual data
  any.SerializeToArray(msg.data<char>() + sizeof(Channel), any.ByteSizeLong());

  socket.send(msg, zmq::send_flags::none);
}

template<typename T>
inline bool ReceiveProto(zmq::socket_t& socket, T& out, bool dont_wait = false) {
  zmq::message_t msg;
  auto flag = dont_wait ? zmq::recv_flags::dontwait : zmq::recv_flags::none;
  if (!socket.recv(msg, flag)) {
    return false;
  }
  google::protobuf::Any any;
  if (msg.size() < sizeof(Channel)) {
    return false;
  }
  auto proto_data = msg.data<char>() + sizeof(Channel);
  auto proto_size = msg.size() - sizeof(Channel);
  if (!any.ParseFromArray(proto_data, proto_size)) {
    return false;
  }
  return any.UnpackTo(&out);
}

} // namespace slog