#include <thread>

#include <gtest/gtest.h>

#include "common/test_utils.h"
#include "connection/channel.h"

using namespace std;
using namespace slog;

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
    std::unique_ptr<ChannelListener> listener(channel_->GetListener());
    MMessage message;
    listener->PollRequest(message);
    proto::Request req;
    message.ToRequest(req);
    ASSERT_EQ("test", req.echo().data());
  });

  MMessage message(MakeEchoRequest("test"));
  message.SetIdentity("zZz");
  channel_->PassToListener(message);
  th.join();
}
