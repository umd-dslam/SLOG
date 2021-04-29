#include "connection/zmq_utils.h"

#include <gtest/gtest.h>

#include <iostream>

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
  req.mutable_ping()->set_time(99);
  SendSerializedProto(push, req);

  Request req2;
  ASSERT_TRUE(RecvDeserializedProto(pull, req2));
  ASSERT_EQ(req2.ping().time(), 99);
}

TEST(ZmqUtilsTest, SendAndReceiveProtoDealerRouter) {
  zmq::context_t context(1);

  zmq::socket_t router(context, ZMQ_ROUTER);
  router.bind("inproc://test");
  zmq::socket_t dealer(context, ZMQ_DEALER);
  dealer.connect("inproc://test");

  // First send from dealer and receive at router
  Request req;
  req.mutable_ping()->set_time(99);
  SendSerializedProtoWithEmptyDelim(dealer, req);

  zmq::message_t identity;
  (void)router.recv(identity);
  Request req2;
  ASSERT_TRUE(RecvDeserializedProtoWithEmptyDelim(router, req2));
  ASSERT_EQ(req2.ping().time(), 99);

  // Then send from router and receive at dealer
  req2.mutable_ping()->set_time(99);
  router.send(identity, zmq::send_flags::sndmore);
  SendSerializedProtoWithEmptyDelim(router, req2);

  ASSERT_TRUE(RecvDeserializedProtoWithEmptyDelim(dealer, req));
  ASSERT_EQ(req.ping().time(), 99);
}

TEST(ZmqUtilsTest, SendAndReceiveWrongProto) {
  zmq::context_t context(1);

  zmq::socket_t push(context, ZMQ_PUSH);
  push.bind("inproc://test");
  zmq::socket_t pull(context, ZMQ_PULL);
  pull.connect("inproc://test");

  Request req;
  req.mutable_ping()->set_time(99);
  SendSerializedProto(push, req);

  Response res;
  ASSERT_FALSE(RecvDeserializedProto(pull, res));
}

TEST(ZmqUtilsTest, ReceiveProtoDontWait) {
  zmq::context_t context(1);

  zmq::socket_t push(context, ZMQ_PUSH);
  push.bind("inproc://test");
  zmq::socket_t pull(context, ZMQ_PULL);
  pull.connect("inproc://test");

  Response res;
  ASSERT_FALSE(RecvDeserializedProto(pull, res, true));
}

TEST(ZmqUtilsTest, SendWithMachineIdAndChannel) {
  zmq::context_t context(1);

  zmq::socket_t push(context, ZMQ_PUSH);
  push.bind("inproc://test");
  zmq::socket_t pull(context, ZMQ_PULL);
  pull.connect("inproc://test");

  Request req;
  req.mutable_ping()->set_time(99);
  SendSerializedProto(push, req, 1, 9);

  zmq::message_t msg;
  (void)pull.recv(msg);
  MachineId machineId;
  ASSERT_TRUE(ParseMachineId(machineId, msg));
  ASSERT_EQ(machineId, 1);

  Channel channel;
  ASSERT_TRUE(ParseChannel(channel, msg));
  ASSERT_EQ(channel, 9);
}

TEST(ZmqUtilsTest, FailedParsing) {
  zmq::message_t msg;
  MachineId id;
  ASSERT_FALSE(ParseMachineId(id, msg));
  Channel chan;
  ASSERT_FALSE(ParseChannel(chan, msg));
  Request req;
  ASSERT_FALSE(DeserializeProto(req, msg));
}