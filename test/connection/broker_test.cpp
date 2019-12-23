#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "common/configuration.h"
#include "common/proto_utils.h"
#include "common/test_utils.h"
#include "connection/broker.h"
#include "proto/internal.pb.h"

using namespace std;
using namespace slog;
using internal::Request;
using internal::Response;

using ConfigVec = vector<shared_ptr<Configuration>>;

ConfigVec MakeTestConfigurations(
    string&& prefix,
    int num_replicas, 
    int num_partitions) {
  int num_machines = num_replicas * num_partitions;
  string addr = "/tmp/test_" + prefix;

  internal::Configuration common_config;
  common_config.set_protocol("ipc");
  common_config.set_broker_port(0);
  common_config.set_num_replicas(num_replicas);
  common_config.set_num_partitions(num_partitions);
  for (int i = 0; i < num_machines; i++) {
    common_config.add_addresses(addr + to_string(i));
  }

  ConfigVec configs;
  configs.reserve(num_machines);

  for (int rep = 0; rep < num_replicas; rep++) {
    for (int part = 0; part < num_partitions; part++) {
      int i = rep * num_partitions + part;
      string local_addr = addr + to_string(i);
      configs.push_back(make_shared<Configuration>(
          common_config,
          local_addr,
          MakeSlogId(rep, part)));
    }
  }
  
  return configs;
}

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
    msg.Add(MakeEchoRequest("ping"));
    msg.Add(RECEIVER);
    msg.SetIdentity(
        SlogIdToString(MakeSlogId(0, 1)));
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
    msg.Set(1, SENDER);
    channel->Send(msg);

    this_thread::sleep_for(1s);
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
    msg.Add(MakeEchoRequest("ping"));
    msg.Add(RECEIVER);
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
    msg.Set(1, SENDER);
    channel->Send(msg);

    this_thread::sleep_for(1s);
  };

  std::thread sender_thread(sender, broker.AddChannel(SENDER));
  std::thread receiver_thread(receiver, broker.AddChannel(RECEIVER));

  broker.StartInNewThread();

  sender_thread.join();
  receiver_thread.join();
}