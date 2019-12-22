#include <gtest/gtest.h>
#include <iostream>
#include "common/test_utils.h"
#include "common/mmessage.h"

using namespace std;
using namespace slog;

const std::string TEST_STRING = "test";

TEST(MMessageTest, AddThenGetRequestProto) {
  MMessage message;
  message.Add(MakeEchoRequest(TEST_STRING));

  proto::Request request;
  ASSERT_TRUE(message.GetProto(request));

  ASSERT_TRUE(request.has_echo_req());
  auto echo = request.echo_req();
  ASSERT_EQ(TEST_STRING, echo.data());
}

TEST(MMessageTest, FromAndToResponseProto) {
  MMessage message;
  message.Add(MakeEchoResponse(TEST_STRING));

  proto::Response response;
  ASSERT_TRUE(message.GetProto(response));

  ASSERT_TRUE(response.has_echo_res());
  auto echo = response.echo_res();
  ASSERT_EQ(TEST_STRING, echo.data());
}

TEST(MMessageTest, SendAndReceive) {
  zmq::context_t context(1);

  zmq::socket_t router(context, ZMQ_ROUTER);
  router.bind("ipc:///tmp/test_mmessage");
  zmq::socket_t dealer(context, ZMQ_DEALER);
  dealer.connect("ipc:///tmp/test_mmessage");

  MMessage message;
  message.Add(MakeEchoRequest(TEST_STRING));
  message.Send(dealer);
  MMessage recv_msg(router);

  proto::Request request;
  ASSERT_TRUE(recv_msg.GetProto(request));
  ASSERT_TRUE(request.has_echo_req());
  auto echo = request.echo_req();
  ASSERT_EQ(TEST_STRING, echo.data());
}