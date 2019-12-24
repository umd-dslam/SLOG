#include <thread>

#include <gtest/gtest.h>

#include "common/test_utils.h"
#include "connection/channel.h"

using namespace std;
using namespace slog;

using internal::Request;
using internal::Response;

class ChannelTest : public ::testing::Test {
protected:

  void SetUp() override {
    auto context = std::make_shared<zmq::context_t>(1);
    channel_ = std::make_unique<Channel>(context, "test_channel");
  }

  std::unique_ptr<Channel> channel_;
};

TEST_F(ChannelTest, ListenToChannel) {
  std::thread th([this](){
    std::unique_ptr<Channel> listener(channel_->GetListener());
    MMessage message;
    listener->Receive(message);
    Request req;
    ASSERT_TRUE(message.GetProto(req));
    ASSERT_EQ("test", req.echo().data());
  });

  MMessage message;
  message.Push(MakeEchoRequest("test"));
  message.SetIdentity("zZz");
  channel_->Send(message);
  th.join();
}

TEST_F(ChannelTest, SendToChannel) {
  std::thread th([this](){
    std::unique_ptr<Channel> listener(channel_->GetListener());
    MMessage message;
    message.SetIdentity("zZz");
    message.Push(MakeEchoResponse("test"));
    listener->Send(message);
  });

  // Not necessary to poll but still do to test GetPollItem
  auto item = channel_->GetPollItem();
  zmq::poll(&item, 1);

  MMessage message;
  channel_->Receive(message);
  Response res;
  ASSERT_TRUE(message.GetProto(res));
  ASSERT_EQ("test", res.echo().data());

  th.join();
}
