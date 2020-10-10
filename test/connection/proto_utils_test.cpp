#include <gtest/gtest.h>
#include <iostream>
#include "connection/proto_utils.h"
#include "proto/internal.pb.h"

using namespace std;
using namespace slog;

using internal::Request;
using internal::Response;

TEST(ProtoUtilsTest, SendAndReceiveProto) {
  zmq::context_t context(1);

  zmq::socket_t push(context, ZMQ_PUSH);
  push.bind("inproc://test");
  zmq::socket_t pull(context, ZMQ_PULL);
  pull.connect("inproc://test");

  Request req;
  req.mutable_echo()->set_data("test");
  SendProto(push, req);

  Request req2;
  ASSERT_TRUE(ReceiveProto(pull, req2));
  ASSERT_EQ(req2.echo().data(), "test");
}

TEST(ProtoUtilsTest, SendAndReceiveWrongProto) {
  zmq::context_t context(1);

  zmq::socket_t push(context, ZMQ_PUSH);
  push.bind("inproc://test");
  zmq::socket_t pull(context, ZMQ_PULL);
  pull.connect("inproc://test");

  Request req;
  req.mutable_echo()->set_data("test");
  SendProto(push, req);

  Response res;
  ASSERT_FALSE(ReceiveProto(pull, res));
}

TEST(ProtoUtilsTest, ReceiveProtoDontWait) {
  zmq::context_t context(1);

  zmq::socket_t push(context, ZMQ_PUSH);
  push.bind("inproc://test");
  zmq::socket_t pull(context, ZMQ_PULL);
  pull.connect("inproc://test");

  Response res;
  ASSERT_FALSE(ReceiveProto(pull, res, true));
}

TEST(ProtoUtilsTest, SendWithChannel) {
  zmq::context_t context(1);

  zmq::socket_t push(context, ZMQ_PUSH);
  push.bind("inproc://test");
  zmq::socket_t pull(context, ZMQ_PULL);
  pull.connect("inproc://test");

  Request req;
  req.mutable_echo()->set_data("test");
  SendProto(push, req, 9);

  zmq::message_t msg;
  pull.recv(msg, zmq::recv_flags::none);
  auto chan = *msg.data<Channel>();
  ASSERT_EQ(chan, 9);
}
