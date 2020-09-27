#include <gtest/gtest.h>
#include <iostream>
#include "common/constants.h"
#include "common/test_utils.h"
#include "common/mmessage.h"

using namespace std;
using namespace slog;

using internal::Request;
using internal::Response;

const string TEST_STRING = "test";

TEST(MMessageTest, AddThenGetRequestProto) {
  MMessage message;
  message.Set(MM_DATA, MakeEchoRequest(TEST_STRING));

  Request request;
  ASSERT_TRUE(message.GetProto(request));

  Response response;
  ASSERT_FALSE(message.GetProto(response));

  ASSERT_TRUE(request.has_echo());
  auto echo = request.echo();
  ASSERT_EQ(TEST_STRING, echo.data());
}

TEST(MMessageTest, FromAndToResponseProto) {
  MMessage message;
  message.Set(MM_DATA, MakeEchoResponse(TEST_STRING));

  Response response;
  ASSERT_TRUE(message.GetProto(response));

  Request request;
  ASSERT_FALSE(message.GetProto(request));

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

  MMessage message;
  message.Set(MM_DATA, MakeEchoRequest(TEST_STRING));
  message.SendTo(dealer);
  MMessage recv_msg(router);

  Request request;
  ASSERT_TRUE(recv_msg.GetProto(request));
  ASSERT_TRUE(request.has_echo());
  auto echo = request.echo();
  ASSERT_EQ(TEST_STRING, echo.data());
}

TEST(MMessageTest, IsProto) {
  MMessage message;
  message.Push(MakeEchoRequest(TEST_STRING));
  message.Push("test_string");

  ASSERT_TRUE(message.IsProto<Request>());
  ASSERT_FALSE(message.IsProto<Response>());
  ASSERT_FALSE(message.IsProto<Request>(1));

  message.Set(0, MakeEchoResponse(TEST_STRING));
  ASSERT_TRUE(message.IsProto<Response>());
}