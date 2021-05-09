#pragma once

#include <google/protobuf/any.pb.h>

#include <sstream>
#include <zmq.hpp>

#include "common/types.h"
#include "proto/internal.pb.h"

namespace slog {

using EnvelopePtr = std::unique_ptr<internal::Envelope>;

inline std::string MakeInProcChannelAddress(Channel chan) { return "inproc://channel_" + std::to_string(chan); }
inline std::string MakeRemoteAddress(const std::string& protocol, const std::string& addr, uint32_t port,
                                     bool binding = false) {
  std::stringstream endpoint;
  endpoint << protocol << "://";
  if (binding && protocol == "tcp") {
    endpoint << "*";
  } else {
    endpoint << addr;
  }
  endpoint << ":" << port;
  return endpoint.str();
}

/**
 * Sends a pointer of an envelope
 */
inline void SendEnvelope(zmq::socket_t& socket, EnvelopePtr&& envelope) {
  auto env = envelope.release();
  zmq::message_t msg(sizeof(env));
  *(msg.data<internal::Envelope*>()) = env;
  socket.send(msg, zmq::send_flags::dontwait);
}

/**
 * Receives a pointer of an envelope
 */
inline EnvelopePtr RecvEnvelope(zmq::socket_t& socket, bool dont_wait = false) {
  zmq::message_t msg;
  auto flag = dont_wait ? zmq::recv_flags::dontwait : zmq::recv_flags::none;
  if (!socket.recv(msg, flag)) {
    return nullptr;
  }
  return EnvelopePtr(*(msg.data<internal::Envelope*>()));
}

inline zmq::message_t SerializeProto(const google::protobuf::Message& proto) {
  google::protobuf::Any any;
  any.PackFrom(proto);

  auto header_sz = sizeof(MachineId) + sizeof(Channel);
  zmq::message_t msg(header_sz + any.ByteSizeLong());
  any.SerializeToArray(msg.data<char>() + header_sz, any.ByteSizeLong());

  return msg;
}

inline void SendAddressedBuffer(zmq::socket_t& socket, zmq::message_t&& msg, MachineId from_machine_id = -1,
                                Channel to_chan = 0) {
  auto machine_id_data = msg.data<MachineId>();
  *machine_id_data = from_machine_id;

  auto channel_data = reinterpret_cast<Channel*>(machine_id_data + 1);
  *channel_data = to_chan;

  socket.send(msg, zmq::send_flags::dontwait);
}

/**
 * Serializes and send proto message. The sent buffer contains
 * <sender machine id> <receiver channel> <proto>
 */
inline void SendSerializedProto(zmq::socket_t& socket, const google::protobuf::Message& proto,
                                MachineId from_machine_id = -1, Channel to_chan = 0) {
  SendAddressedBuffer(socket, SerializeProto(proto), from_machine_id, to_chan);
}

inline void SendSerializedProtoWithEmptyDelim(zmq::socket_t& socket, const google::protobuf::Message& proto) {
  socket.send(zmq::message_t{}, zmq::send_flags::sndmore);
  SendSerializedProto(socket, proto);
}

inline bool ParseMachineId(MachineId& id, const zmq::message_t& msg) {
  if (msg.size() < sizeof(MachineId)) {
    return false;
  }
  id = *msg.data<MachineId>();
  return true;
}

inline bool ParseChannel(Channel& chan, const zmq::message_t& msg) {
  if (msg.size() < sizeof(MachineId) + sizeof(Channel)) {
    return false;
  }
  auto chan_data = reinterpret_cast<const Channel*>(msg.data<MachineId>() + 1);
  chan = *chan_data;
  return true;
}

template <typename T>
inline bool DeserializeProto(T& out, const char* data, size_t size) {
  google::protobuf::Any any;
  auto header_sz = sizeof(MachineId) + sizeof(Channel);
  if (size < header_sz) {
    return false;
  }
  // Skip the machineid and channel part
  auto proto_data = data + header_sz;
  auto proto_size = size - header_sz;
  if (!any.ParseFromArray(proto_data, proto_size)) {
    return false;
  }
  return any.UnpackTo(&out);
}

template <typename T>
inline bool DeserializeProto(T& out, const zmq::message_t& msg) {
  return DeserializeProto(out, msg.data<char>(), msg.size());
}

template <typename T>
inline bool RecvDeserializedProto(zmq::socket_t& socket, T& out, bool dont_wait = false) {
  zmq::message_t msg;
  auto flag = dont_wait ? zmq::recv_flags::dontwait : zmq::recv_flags::none;
  if (!socket.recv(msg, flag)) {
    return false;
  }
  return DeserializeProto(out, msg);
}

template <typename T>
inline bool RecvDeserializedProtoWithEmptyDelim(zmq::socket_t& socket, T& out, bool dont_wait = false) {
  if (zmq::message_t empty; !socket.recv(empty) || !empty.more()) {
    return false;
  }
  return RecvDeserializedProto(socket, out, dont_wait);
}

inline EnvelopePtr DeserializeEnvelope(const zmq::message_t& msg) {
  auto env = std::make_unique<internal::Envelope>();
  if (!DeserializeProto(*env, msg)) {
    return nullptr;
  }
  MachineId machine_id = -1;
  ParseMachineId(machine_id, msg);
  env->set_from(machine_id);
  return env;
}

}  // namespace slog