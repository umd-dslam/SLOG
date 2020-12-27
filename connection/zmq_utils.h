#pragma once

#include <google/protobuf/any.pb.h>

#include <zmq.hpp>

#include "common/types.h"

namespace slog {

/**
 * The format of a message is:
 * <sender machine id> <receiver channel> <proto>
 */

inline void SendProto(zmq::socket_t& socket, const google::protobuf::Message& proto, Channel chan = 0,
                      MachineId machineId = -1) {
  google::protobuf::Any any;
  any.PackFrom(proto);

  size_t sz = sizeof(MachineId) + sizeof(Channel) + any.ByteSizeLong();
  zmq::message_t msg(sz);

  auto data = msg.data<char>();
  memcpy(data, &machineId, sizeof(MachineId));
  data += sizeof(MachineId);
  memcpy(data, &chan, sizeof(Channel));
  data += sizeof(Channel);
  any.SerializeToArray(data, any.ByteSizeLong());

  socket.send(msg, zmq::send_flags::none);
}

inline void SendProtoWithEmptyDelimiter(zmq::socket_t& socket, const google::protobuf::Message& proto) {
  socket.send(zmq::message_t{}, zmq::send_flags::sndmore);
  SendProto(socket, proto);
}

inline bool ParseMachineId(MachineId& id, const zmq::message_t& msg) {
  if (msg.size() < sizeof(MachineId)) {
    return false;
  }
  memcpy(&id, msg.data<char>(), sizeof(MachineId));
  return true;
}

inline bool ParseChannel(Channel& chan, const zmq::message_t& msg) {
  if (msg.size() < sizeof(MachineId) + sizeof(Channel)) {
    return false;
  }
  memcpy(&chan, msg.data<char>() + sizeof(MachineId), sizeof(Channel));
  return true;
}

inline bool ParseAny(google::protobuf::Any& any, const zmq::message_t& msg) {
  auto header_sz = sizeof(MachineId) + sizeof(Channel);
  if (msg.size() < header_sz) {
    return false;
  }
  // Skip the machineid and channel part
  auto proto_data = msg.data<char>() + header_sz;
  auto proto_size = msg.size() - header_sz;
  if (!any.ParseFromArray(proto_data, proto_size)) {
    return false;
  }
  return true;
}

template <typename T>
inline bool ParseProto(T& out, const zmq::message_t& msg) {
  google::protobuf::Any any;
  if (!ParseAny(any, msg)) {
    return false;
  }
  return any.UnpackTo(&out);
}

template <typename T>
inline bool ReceiveProto(zmq::socket_t& socket, T& out, bool dont_wait = false) {
  zmq::message_t msg;
  auto flag = dont_wait ? zmq::recv_flags::dontwait : zmq::recv_flags::none;
  if (!socket.recv(msg, flag)) {
    return false;
  }
  return ParseProto(out, msg);
}

template <typename T>
inline bool ReceiveProtoWithEmptyDelimiter(zmq::socket_t& socket, T& out, bool dont_wait = false) {
  if (zmq::message_t empty; !socket.recv(empty) || !empty.more()) {
    return false;
  }
  return ReceiveProto(socket, out, dont_wait);
}

}  // namespace slog