#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "common/configuration.h"
#include "common/proto_utils.h"
#include "common/test_utils.h"
#include "connection/broker.h"
#include "proto/config.pb.h"

using namespace std;
using namespace slog;

using ConfigVec = vector<shared_ptr<Configuration>>;

ConfigVec MakeTestConfigurations(
    int num_replicas, 
    int num_partitions) {
  int num_machines = num_replicas * num_partitions;

  proto::Configuration common_config;
  common_config.set_protocol("ipc");
  common_config.set_broker_port(0);
  common_config.set_num_replicas(num_replicas);
  common_config.set_num_partitions(num_partitions);
  string addr = "/tmp/test_";
  for (int i = 0; i < num_machines; i++) {
    common_config.add_addresses(addr + to_string(i));
  }

  ConfigVec configs;
  configs.reserve(num_machines);

  proto::SlogIdentifier slog_id;
  for (int rep = 0; rep < num_replicas; rep++) {
    for (int part = 0; part < num_partitions; part++) {
      int i = rep * num_partitions + part;
      slog_id.set_replica(rep);
      slog_id.set_partition(part);
      string local_addr = addr + to_string(i);
      configs.push_back(make_shared<Configuration>(
          common_config,
          local_addr,
          slog_id));
    }
  }
  
  return configs;
}

TEST(BrokerTest, PingPong) {
  ConfigVec configs = MakeTestConfigurations(1, 2);

  auto sender = thread([&configs]() {
    auto context = std::make_shared<zmq::context_t>(1);
    Broker broker(configs[0], context);

    unique_ptr<ChannelListener> channel(
        broker.AddChannel(Broker::SEQUENCER_CHANNEL));

    broker.Start();

    // Send ping
    MMessage msg(MakeEchoRequest("ping"));
    msg.SetChannel(Broker::SCHEDULER_CHANNEL);
    msg.SetIdentity(
        MakeSlogIdentifier(0, 1).SerializeAsString());
    channel->SendMessage(std::move(msg));

    // Wait for pong
    ASSERT_TRUE(channel->PollMessage(msg, 2000));
    proto::Response res;
    ASSERT_TRUE(msg.ToResponse(res));
    ASSERT_EQ("pong", res.echo_res().data());
  });

  auto receiver = thread([&configs]() {
    auto context = std::make_shared<zmq::context_t>(1);
    Broker broker(configs[1], context);
    std::unique_ptr<ChannelListener> channel(
        broker.AddChannel(Broker::SCHEDULER_CHANNEL));

    broker.Start();

    // Wait for ping
    MMessage msg;
    ASSERT_TRUE(channel->PollMessage(msg, 2000));
    proto::Request req;
    ASSERT_TRUE(msg.ToRequest(req));
    ASSERT_EQ("ping", req.echo_req().data());

    // Send pong
    proto::Response res;
    res.mutable_echo_res()->set_data("pong");
    msg.SetChannel(Broker::SEQUENCER_CHANNEL);
    msg.SetResponse(res);
    channel->SendMessage(msg);

    this_thread::sleep_for(1s);
  });

  sender.join();
  receiver.join();
}