#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/proto_utils.h"
#include "common/test_utils.h"
#include "connection/broker.h"
#include "connection/sender.h"
#include "connection/zmq_utils.h"
#include "proto/internal.pb.h"

using namespace std;
using namespace slog;
using internal::Request;
using internal::Response;

zmq::socket_t MakePullSocket(zmq::context_t& context, Channel chan) {
  zmq::socket_t socket(context, ZMQ_PULL);
  socket.bind("inproc://channel_" + to_string(chan));
  socket.setsockopt(ZMQ_LINGER, 0);
  return socket;
}

TEST(BrokerAndSenderTest, PingPong) {
  const Channel PING = 10;
  const Channel PONG = 11;
  ConfigVec configs = MakeTestConfigurations("pingpong", 1, 2);

  auto ping = thread([&]() {
    auto context = make_shared<zmq::context_t>(1);

    auto socket = MakePullSocket(*context, PING);

    auto broker = make_shared<Broker>(configs[0], context);
    broker->AddChannel(PING);
    broker->StartInNewThread();

    Sender sender(broker);
    // Send ping
    sender.Send(MakeEchoRequest("ping"), PONG, configs[0]->MakeMachineIdNum(0, 1));

    // Wait for pong
    Response res;
    ASSERT_TRUE(ReceiveProto(socket, res));
    ASSERT_EQ("pong", res.echo().data());
  });

  auto pong = thread([&]() {
    auto context = std::make_shared<zmq::context_t>(1);

    auto socket = MakePullSocket(*context, PONG);

    auto broker = make_shared<Broker>(configs[1], context);
    broker->AddChannel(PONG);
    broker->StartInNewThread();

    Sender sender(broker);

    // Wait for ping
    Request req;
    ASSERT_TRUE(ReceiveProto(socket, req));
    ASSERT_EQ("ping", req.echo().data());

    // Send pong
    sender.Send(MakeEchoResponse("pong"), PING, configs[1]->MakeMachineIdNum(0, 0));

    this_thread::sleep_for(200ms);
  });

  ping.join();
  pong.join();
}

TEST(BrokerTest, LocalPingPong) {
  const Channel PING = 10;
  const Channel PONG = 11;
  ConfigVec configs = MakeTestConfigurations("local_ping_pong", 1, 1);
  auto context = std::make_shared<zmq::context_t>(1);
  auto broker = make_shared<Broker>(configs[0], context);
  broker->AddChannel(PING);
  broker->AddChannel(PONG);

  broker->StartInNewThread();

  auto ping = thread([&]() {
    Sender sender(broker);
    auto socket = MakePullSocket(*context, PING);

    // Send ping
    sender.Send(MakeEchoRequest("ping"), PONG);

    // Wait for pong
    Response res;
    ASSERT_TRUE(ReceiveProto(socket, res));
    ASSERT_EQ("pong", res.echo().data());
  });

  auto pong = thread([&]() {
    Sender sender(broker);
    auto socket = MakePullSocket(*context, PONG);

    // Wait for ping
    Request req;
    ASSERT_TRUE(ReceiveProto(socket, req));
    ASSERT_EQ("ping", req.echo().data());

    // Send pong
    sender.Send(MakeEchoResponse("pong"), PING);
    this_thread::sleep_for(200ms);
  });

  ping.join();
  pong.join();
}