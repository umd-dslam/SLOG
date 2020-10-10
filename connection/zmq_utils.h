#pragma once

#include <zmq.hpp>
#include <google/protobuf/any.pb.h>

#include "common/types.h"

namespace slog {

/**
 * The format of a message is: 
 * <sender machine id> <receiver channel> <proto>
 */

inline void SendProto(
    zmq::socket_t& socket,
    const google::protobuf::Message& proto,
    Channel chan = 0,
    MachineIdNum machineId = -1) {
  google::protobuf::Any any;
  any.PackFrom(proto);
  
  size_t sz = sizeof(MachineIdNum) + sizeof(Channel) + any.ByteSizeLong();
  zmq::message_t msg(sz);

  auto data = msg.data<char>();
  memcpy(data, &machineId, sizeof(MachineIdNum));
  data += sizeof(MachineIdNum);
  memcpy(data, &chan, sizeof(Channel));
  data += sizeof(Channel);
  any.SerializeToArray(data, any.ByteSizeLong());

  socket.send(msg, zmq::send_flags::none);
}

inline bool ParseMachineId(MachineIdNum& id, const zmq::message_t& msg) {
  if (msg.size() < sizeof(MachineIdNum)) {
    return false;
  }
  id = *msg.data<MachineIdNum>();
  return true;
}

inline bool ParseChannel(Channel& chan, const zmq::message_t& msg) {
  if (msg.size() < sizeof(MachineIdNum) + sizeof(Channel)) {
    return false;
  }
  chan = *reinterpret_cast<const Channel*>(msg.data<char>() + sizeof(MachineIdNum));
  return true;
}

template<typename T>
inline bool ParseProto(T& out, const zmq::message_t& msg) {
  auto header_sz = sizeof(MachineIdNum) + sizeof(Channel);
  google::protobuf::Any any;
  if (msg.size() < header_sz) {
    return false;
  }
  // Skip the machineid and channel part
  auto proto_data = msg.data<char>() + header_sz;
  auto proto_size = msg.size() - header_sz;
  if (!any.ParseFromArray(proto_data, proto_size)) {
    return false;
  }
  return any.UnpackTo(&out);  
}

template<typename T>
inline bool ReceiveProto(zmq::socket_t& socket, T& out, bool dont_wait = false) {
  zmq::message_t msg;
  auto flag = dont_wait ? zmq::recv_flags::dontwait : zmq::recv_flags::none;
  if (!socket.recv(msg, flag)) {
    return false;
  }
  return ParseProto(out, msg);
}

} // namespace slog