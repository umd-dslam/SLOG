#include <gtest/gtest.h>
#include <iostream>
#include "common/test_utils.h"
#include "common/mmessage.h"

using namespace std;
using namespace slog;

const std::string TEST_STRING = "test";

TEST(MMessageTest, FromAndToRequestProto) {
  MMessage message(MakeEchoRequest(TEST_STRING));

  proto::Request request;
  ASSERT_TRUE(message.ToRequest(request));

  proto::Response response;
  ASSERT_FALSE(message.ToResponse(response));

  ASSERT_TRUE(request.has_echo());
  auto echo = request.echo();
  ASSERT_EQ(TEST_STRING, echo.data());
}

TEST(MMessageTest, FromAndToResponseProto) {
  MMessage message(MakeEchoResponse(TEST_STRING));

  proto::Response response;
  ASSERT_TRUE(message.ToResponse(response));

  proto::Request request;
  ASSERT_FALSE(message.ToRequest(request));

  ASSERT_TRUE(response.has_echo());
  auto echo = response.echo();
  ASSERT_EQ(TEST_STRING, echo.data());
}

TEST(MMessageTest, SendAndReceive) {
  zmq::context_t context(1);

  zmq::socket_t router(context, ZMQ_ROUTER);
  router.bind("ipc:///tmp/test_mmessage");
  zmq::socket_t dealer(context, ZMQ_DEALER);
  dealer.connect("ipc:///tmp/test_mmessage");

  MMessage message(MakeEchoRequest(TEST_STRING));
  message.Send(dealer);
  MMessage recv_msg(router);

  proto::Request request;
  ASSERT_TRUE(recv_msg.ToRequest(request));
  ASSERT_TRUE(request.has_echo());
  auto echo = request.echo();
  ASSERT_EQ(TEST_STRING, echo.data());
}