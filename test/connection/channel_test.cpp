#include <thread>

#include <gtest/gtest.h>

#include "common/constants.h"
#include "common/test_utils.h"
#include "connection/channel.h"

using namespace std;
using namespace slog;

using internal::Request;
using internal::Response;

class ChannelTest : public ::testing::Test {
protected:
  void SetUp() override {
    auto context = make_shared<zmq::context_t>(1);
    channel = make_unique<Channel>(context, "test_channel");
  }

  unique_ptr<Channel> channel;
};

TEST_F(ChannelTest, ListenToChannel) {
  thread th([this](){
    unique_ptr<Channel> listener(channel->GetListener());
    MMessage message;
    listener->Receive(message);
    Request req;
    ASSERT_TRUE(message.GetProto(req));
    ASSERT_EQ("test", req.echo().data());
  });

  MMessage message;
  message.Set(MM_PROTO, MakeEchoRequest("test"));
  message.SetIdentity("zZz");
  channel->Send(message);
  th.join();
}

TEST_F(ChannelTest, SendToChannel) {
  thread th([this](){
    unique_ptr<Channel> listener(channel->GetListener());
    MMessage message;
    message.SetIdentity("zZz");
    message.Set(MM_PROTO, MakeEchoResponse("test"));
    listener->Send(message);
  });

  // Not necessary to poll but still do to test GetPollItem
  auto item = channel->GetPollItem();
  zmq::poll(&item, 1);

  MMessage message;
  channel->Receive(message);
  Response res;
  ASSERT_TRUE(message.GetProto(res));
  ASSERT_EQ("test", res.echo().data());

  th.join();
}

TEST_F(ChannelTest, MultiSendToChannel) {
  thread th([this](){
    unique_ptr<Channel> listener(channel->GetListener());
    MMessage message;
    message.SetIdentity("zZz");
    message.Set(MM_PROTO, MakeEchoResponse("foo"));
    listener->Send(message, true);

    message.SetIdentity("zZz");
    message.Set(MM_PROTO, MakeEchoResponse("bar"));
    listener->Send(message, true);

    message.SetIdentity("zZz");
    message.Set(MM_PROTO, MakeEchoResponse("far"));
    listener->Send(message, false);
  });

  MMessage message;
  Response res;

  ASSERT_TRUE(channel->Receive(message));
  ASSERT_TRUE(message.GetProto(res));
  ASSERT_EQ("foo", res.echo().data());

  ASSERT_TRUE(channel->Receive(message));
  ASSERT_TRUE(message.GetProto(res));
  ASSERT_EQ("bar", res.echo().data());

  ASSERT_FALSE(channel->Receive(message));
  ASSERT_TRUE(message.GetProto(res));
  ASSERT_EQ("far", res.echo().data());

  th.join();
}
