#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/proto_utils.h"
#include "connection/broker.h"
#include "connection/sender.h"
#include "connection/zmq_utils.h"
#include "proto/internal.pb.h"
#include "test/test_utils.h"

using namespace std;
using namespace slog;
using internal::Envelope;
using internal::Request;
using internal::Response;

zmq::socket_t MakePullSocket(zmq::context_t& context, Channel chan) {
  zmq::socket_t socket(context, ZMQ_PULL);
  socket.bind("inproc://channel_" + to_string(chan));
  return socket;
}

EnvelopePtr MakeEchoRequest(const std::string& data) {
  auto env = std::make_unique<internal::Envelope>();
  auto echo = env->mutable_request()->mutable_echo();
  echo->set_data(data);
  return env;
}

EnvelopePtr MakeEchoResponse(const std::string& data) {
  auto env = std::make_unique<internal::Envelope>();
  auto echo = env->mutable_response()->mutable_echo();
  echo->set_data(data);
  return env;
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
    auto ping_req = MakeEchoRequest("ping");
    sender.SendSerialized(*ping_req, configs[0]->MakeMachineId(0, 1), PONG);

    // Wait for pong
    auto res = RecvEnvelope(socket);
    ASSERT_TRUE(res != nullptr);
    ASSERT_TRUE(res->has_response());
    ASSERT_EQ("pong", res->response().echo().data());
  });

  auto pong = thread([&]() {
    auto context = std::make_shared<zmq::context_t>(1);

    auto socket = MakePullSocket(*context, PONG);

    auto broker = make_shared<Broker>(configs[1], context);
    broker->AddChannel(PONG);
    broker->StartInNewThread();

    Sender sender(broker);

    // Wait for ping
    auto req = RecvEnvelope(socket);
    ASSERT_TRUE(req != nullptr);
    ASSERT_TRUE(req->has_request());
    ASSERT_EQ("ping", req->request().echo().data());

    // Send pong
    auto pong_res = MakeEchoResponse("pong");
    sender.SendSerialized(*pong_res, configs[1]->MakeMachineId(0, 0), PING);

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
    sender.SendLocal(MakeEchoRequest("ping"), PONG);

    // Wait for pong
    auto res = RecvEnvelope(socket);
    ASSERT_TRUE(res != nullptr);
    ASSERT_EQ("pong", res->response().echo().data());
  });

  auto pong = thread([&]() {
    Sender sender(broker);
    auto socket = MakePullSocket(*context, PONG);

    // Wait for ping
    auto req = RecvEnvelope(socket);
    ASSERT_TRUE(req != nullptr);
    ASSERT_EQ("ping", req->request().echo().data());

    // Send pong
    sender.SendLocal(MakeEchoResponse("pong"), PING);
    this_thread::sleep_for(200ms);
  });

  ping.join();
  pong.join();
}