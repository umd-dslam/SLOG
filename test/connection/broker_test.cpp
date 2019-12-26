#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/proto_utils.h"
#include "common/test_utils.h"
#include "connection/broker.h"
#include "proto/internal.pb.h"

using namespace std;
using namespace slog;
using internal::Request;
using internal::Response;

TEST(BrokerTest, PingPong) {
  const string SENDER("sender");
  const string RECEIVER("receiver");
  ConfigVec configs = MakeTestConfigurations("pingpong", 1, 2);

  auto sender = thread([&]() {
    auto context = std::make_shared<zmq::context_t>(1);
    Broker broker(configs[0], context);

    unique_ptr<Channel> channel(
        broker.AddChannel(SENDER));

    broker.StartInNewThread();

    // Send ping
    MMessage msg;
    msg.Set(MM_REQUEST, MakeEchoRequest("ping"));
    msg.Set(MM_FROM_CHANNEL, SENDER);
    msg.Set(MM_TO_CHANNEL, RECEIVER);
    msg.SetIdentity(MakeMachineId(0, 1));
    channel->Send(std::move(msg));

    // Wait for pong
    channel->Receive(msg);
    Response res;
    ASSERT_TRUE(msg.GetProto(res));
    ASSERT_EQ("pong", res.echo().data());
  });

  auto receiver = thread([&]() {
    auto context = std::make_shared<zmq::context_t>(1);
    Broker broker(configs[1], context);
    std::unique_ptr<Channel> channel(
        broker.AddChannel(RECEIVER));

    broker.StartInNewThread();

    // Wait for ping
    MMessage msg;
    channel->Receive(msg);
    Request req;
    ASSERT_TRUE(msg.GetProto(req));
    ASSERT_EQ("ping", req.echo().data());

    // Send pong
    Response res;
    res.mutable_echo()->set_data("pong");
    msg.Set(0, res);
    channel->Send(msg);

    this_thread::sleep_for(200ms);
  });

  sender.join();
  receiver.join();
}


TEST(BrokerTest, InterchannelPingPong) {
  const string SENDER("sender");
  const string RECEIVER("receiver");
  ConfigVec configs = MakeTestConfigurations("interchannel_ping_pong", 1, 1);
  auto context = std::make_shared<zmq::context_t>(1);
  Broker broker(configs[0], context);
  
  auto sender = [&](Channel* listener) {
    unique_ptr<Channel> channel(listener);

    // Send ping
    MMessage msg;
    msg.Set(MM_REQUEST, MakeEchoRequest("ping"));
    msg.Set(MM_FROM_CHANNEL, SENDER);
    msg.Set(MM_TO_CHANNEL, RECEIVER);
    channel->Send(std::move(msg));

    // Wait for pong
    channel->Receive(msg);
    Response res;
    ASSERT_TRUE(msg.GetProto(res));
    ASSERT_EQ("pong", res.echo().data());
  };

  auto receiver = [&](Channel* listener) {
    unique_ptr<Channel> channel(listener);

    // Wait for ping
    MMessage msg;
    channel->Receive(msg);
    Request req;
    ASSERT_TRUE(msg.GetProto(req));
    ASSERT_EQ("ping", req.echo().data());

    // Send pong
    Response res;
    res.mutable_echo()->set_data("pong");
    msg.SetIdentity("");
    msg.Set(0, res);
    channel->Send(msg);

    this_thread::sleep_for(200ms);
  };

  std::thread sender_thread(sender, broker.AddChannel(SENDER));
  std::thread receiver_thread(receiver, broker.AddChannel(RECEIVER));

  broker.StartInNewThread();

  sender_thread.join();
  receiver_thread.join();
}