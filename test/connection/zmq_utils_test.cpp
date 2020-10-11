#include <gtest/gtest.h>
#include <iostream>
#include "connection/zmq_utils.h"
#include "proto/internal.pb.h"

using namespace std;
using namespace slog;

using internal::Request;
using internal::Response;

TEST(ZmqUtilsTest, SendAndReceiveProto) {
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

TEST(ZmqUtilsTest, SendAndReceiveProtoDealerRouter) {
  zmq::context_t context(1);

  zmq::socket_t router(context, ZMQ_ROUTER);
  router.bind("inproc://test");
  zmq::socket_t dealer(context, ZMQ_DEALER);
  dealer.connect("inproc://test");

  // First send from dealer and receive at router
  Request req;
  req.mutable_echo()->set_data("test");
  SendProtoWithEmptyDelimiter(dealer, req);

  zmq::message_t identity;
  (void)router.recv(identity);
  Request req2;
  ASSERT_TRUE(ReceiveProtoWithEmptyDelimiter(router, req2));
  ASSERT_EQ(req2.echo().data(), "test");

  // Then send from router and receive at dealer
  req2.mutable_echo()->set_data("test2");
  router.send(identity, zmq::send_flags::sndmore);
  SendProtoWithEmptyDelimiter(router, req2);

  ASSERT_TRUE(ReceiveProtoWithEmptyDelimiter(dealer, req));
  ASSERT_EQ(req.echo().data(), "test2");
}

TEST(ZmqUtilsTest, SendAndReceiveWrongProto) {
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

TEST(ZmqUtilsTest, ReceiveProtoDontWait) {
  zmq::context_t context(1);

  zmq::socket_t push(context, ZMQ_PUSH);
  push.bind("inproc://test");
  zmq::socket_t pull(context, ZMQ_PULL);
  pull.connect("inproc://test");

  Response res;
  ASSERT_FALSE(ReceiveProto(pull, res, true));
}

TEST(ZmqUtilsTest, SendWithMachineIdAndChannel) {
  zmq::context_t context(1);

  zmq::socket_t push(context, ZMQ_PUSH);
  push.bind("inproc://test");
  zmq::socket_t pull(context, ZMQ_PULL);
  pull.connect("inproc://test");

  Request req;
  req.mutable_echo()->set_data("test");
  SendProto(push, req, 9, 1);

  zmq::message_t msg;
  (void)pull.recv(msg);
  MachineIdNum machineId;
  ASSERT_TRUE(ParseMachineId(machineId, msg));
  ASSERT_EQ(machineId, 1);

  Channel channel;
  ASSERT_TRUE(ParseChannel(channel, msg));
  ASSERT_EQ(channel, 9);
}

TEST(ZmqUtilsTest, FailedParsing) {
  zmq::message_t msg;
  MachineIdNum id;
  ASSERT_FALSE(ParseMachineId(id, msg));
  Channel chan;
  ASSERT_FALSE(ParseChannel(chan, msg));
  Request req;
  ASSERT_FALSE(ParseProto(req, msg));
}