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
  socket.bind(MakeInProcChannelAddress(chan));
  return socket;
}

EnvelopePtr MakePing(int64_t time) {
  auto env = std::make_unique<internal::Envelope>();
  env->mutable_request()->mutable_ping()->set_time(time);
  return env;
}

EnvelopePtr MakePong(int64_t time) {
  auto env = std::make_unique<internal::Envelope>();
  env->mutable_response()->mutable_pong()->set_time(time);
  return env;
}

TEST(BrokerAndSenderTest, PingPong) {
  const Channel PING = 8;
  const Channel PONG = 9;
  ConfigVec configs = MakeTestConfigurations("pingpong", 1, 2);

  auto ping = thread([&]() {
    auto broker = Broker::New(configs[0], kTestModuleTimeout);
    broker->AddChannel(PING);
    broker->StartInNewThreads();

    auto recv_socket = MakePullSocket(*broker->context(), PING);

    Sender sender(broker->config(), broker->context());
    // Send ping
    auto ping_req = MakePing(99);
    sender.Send(*ping_req, configs[0]->MakeMachineId(0, 1), PONG);

    // Wait for pong
    auto res = RecvEnvelope(recv_socket);
    ASSERT_TRUE(res != nullptr);
    ASSERT_TRUE(res->has_response());
    ASSERT_EQ(99, res->response().pong().time());
  });

  auto pong = thread([&]() {
    // Set blocky to true to avoid exitting before sending the pong message
    auto broker = Broker::New(configs[1], kTestModuleTimeout, true);
    broker->AddChannel(PONG);
    broker->StartInNewThreads();

    auto socket = MakePullSocket(*broker->context(), PONG);

    Sender sender(broker->config(), broker->context());

    // Wait for ping
    auto req = RecvEnvelope(socket);
    ASSERT_TRUE(req != nullptr);
    ASSERT_TRUE(req->has_request());
    ASSERT_EQ(99, req->request().ping().time());

    // Send pong
    auto pong_res = MakePong(99);
    sender.Send(*pong_res, configs[1]->MakeMachineId(0, 0), PING);
  });

  ping.join();
  pong.join();
}

TEST(BrokerTest, LocalPingPong) {
  const Channel PING = 8;
  const Channel PONG = 9;
  ConfigVec configs = MakeTestConfigurations("local_ping_pong", 1, 1);
  auto broker = Broker::New(configs[0], kTestModuleTimeout);
  broker->AddChannel(PING);
  broker->AddChannel(PONG);

  broker->StartInNewThreads();

  auto ping = thread([&]() {
    Sender sender(broker->config(), broker->context());
    auto socket = MakePullSocket(*broker->context(), PING);

    // Send ping
    sender.Send(MakePing(99), PONG);

    // Wait for pong
    auto res = RecvEnvelope(socket);
    ASSERT_TRUE(res != nullptr);
    ASSERT_EQ(99, res->response().pong().time());
  });

  auto pong = thread([&]() {
    Sender sender(broker->config(), broker->context());
    auto socket = MakePullSocket(*broker->context(), PONG);

    // Wait for ping
    auto req = RecvEnvelope(socket);
    ASSERT_TRUE(req != nullptr);
    ASSERT_EQ(99, req->request().ping().time());

    // Send pong
    sender.Send(MakePong(99), PING);
  });

  ping.join();
  pong.join();
}

TEST(BrokerTest, MultiSend) {
  const Channel PING = 8;
  const Channel PONG = 9;
  const int NUM_PONGS = 3;
  ConfigVec configs = MakeTestConfigurations("pingpong", 1, NUM_PONGS + 1);

  auto ping = thread([&]() {
    auto broker = Broker::New(configs[0], kTestModuleTimeout);
    broker->AddChannel(PING);
    broker->StartInNewThreads();

    auto socket = MakePullSocket(*broker->context(), PING);

    Sender sender(broker->config(), broker->context());
    // Send ping
    auto ping_req = MakePing(99);
    vector<MachineId> dests;
    for (int i = 0; i < NUM_PONGS; i++) {
      dests.push_back(configs[0]->MakeMachineId(0, i + 1));
    }
    sender.Send(*ping_req, dests, PONG);

    // Wait for pongs
    for (int i = 0; i < NUM_PONGS; i++) {
      auto res = RecvEnvelope(socket);
      ASSERT_TRUE(res != nullptr);
      ASSERT_TRUE(res->has_response());
      ASSERT_EQ(99, res->response().pong().time());
    }
  });

  thread pongs[NUM_PONGS];
  for (int i = 0; i < NUM_PONGS; i++) {
    pongs[i] = thread([&configs, i]() {
      auto broker = Broker::New(configs[i + 1], kTestModuleTimeout);
      broker->AddChannel(PONG);
      broker->StartInNewThreads();

      auto socket = MakePullSocket(*broker->context(), PONG);

      Sender sender(broker->config(), broker->context());

      // Wait for ping
      auto req = RecvEnvelope(socket);
      ASSERT_TRUE(req != nullptr);
      ASSERT_TRUE(req->has_request());
      ASSERT_EQ(99, req->request().ping().time());

      // Send pong
      auto pong_res = MakePong(99);
      sender.Send(*pong_res, configs[i + 1]->MakeMachineId(0, 0), PING);

      this_thread::sleep_for(200ms);
    });
  }

  ping.join();
  for (int i = 0; i < NUM_PONGS; i++) {
    pongs[i].join();
  }
}

TEST(BrokerTest, CreateRedirection) {
  const Channel PING = 8;
  const Channel PONG = 9;
  const Channel TAG = 11111;
  ConfigVec configs = MakeTestConfigurations("pingpong", 1, 2);

  // Initialize ping machine
  auto ping_broker = Broker::New(configs[0], kTestModuleTimeout);
  auto ping_socket = MakePullSocket(*ping_broker->context(), PING);
  ping_broker->AddChannel(PING);
  ping_broker->StartInNewThreads();
  Sender ping_sender(ping_broker->config(), ping_broker->context());

  // Establish a redirection from TAG to the PING channel at the ping machine.
  // We do it early so that hopefully the redirection is established by the time
  // we receive the pong message
  {
    auto env = std::make_unique<internal::Envelope>();
    auto redirect = env->mutable_request()->mutable_broker_redirect();
    redirect->set_tag(TAG);
    redirect->set_channel(PING);
    ping_sender.Send(move(env), kBrokerChannel + 1);
  }

  // Initialize pong machine
  auto pong_broker = Broker::New(configs[1], kTestModuleTimeout);
  auto pong_socket = MakePullSocket(*pong_broker->context(), PONG);
  pong_broker->AddChannel(PONG);
  pong_broker->StartInNewThreads();
  Sender pong_sender(pong_broker->config(), pong_broker->context());

  // Send ping message with a tag of the pong machine.
  {
    auto ping_req = MakePing(99);
    ping_sender.Send(*ping_req, configs[0]->MakeMachineId(0, 1), TAG);
  }

  // The pong machine does not know which channel to forward to yet at this point
  // so the message will be queued up at the broker
  this_thread::sleep_for(5ms);
  ASSERT_EQ(RecvEnvelope(pong_socket, true), nullptr);

  // Establish a redirection from TAG to the PONG channel at the pong machine
  {
    auto env = std::make_unique<internal::Envelope>();
    auto redirect = env->mutable_request()->mutable_broker_redirect();
    redirect->set_tag(TAG);
    redirect->set_channel(PONG);
    pong_sender.Send(move(env), kBrokerChannel + 1);
  }

  // Now we can receive the ping message
  {
    auto ping_req = RecvEnvelope(pong_socket);
    ASSERT_TRUE(ping_req != nullptr);
    ASSERT_TRUE(ping_req->has_request());
    ASSERT_EQ(99, ping_req->request().ping().time());
  }

  // Send pong
  {
    auto pong_res = MakePong(99);
    pong_sender.Send(*pong_res, configs[1]->MakeMachineId(0, 0), TAG);
  }

  // We should be able to receive pong here since we already establish a redirection at
  // the beginning for the ping machine
  {
    auto pong_res = RecvEnvelope(ping_socket);
    ASSERT_TRUE(pong_res != nullptr);
    ASSERT_TRUE(pong_res->has_response());
    ASSERT_EQ(99, pong_res->response().pong().time());
  }
}

TEST(BrokerTest, RemoveRedirection) {
  const Channel PING = 8;
  const Channel PONG = 9;
  const Channel TAG = 11111;
  ConfigVec configs = MakeTestConfigurations("pingpong", 1, 2);

  // Initialize ping machine
  auto ping_broker = Broker::New(configs[0], kTestModuleTimeout);
  auto ping_socket = MakePullSocket(*ping_broker->context(), PING);
  ping_broker->AddChannel(PING);
  ping_broker->StartInNewThreads();
  Sender ping_sender(ping_broker->config(), ping_broker->context());

  // Initialize pong machine
  auto pong_broker = Broker::New(configs[1], kTestModuleTimeout);
  auto pong_socket = MakePullSocket(*pong_broker->context(), PONG);
  pong_broker->AddChannel(PONG);
  pong_broker->StartInNewThreads();
  Sender pong_sender(pong_broker->config(), pong_broker->context());

  // Establish a redirection from TAG to the PONG channel at the pong machine
  {
    auto env = std::make_unique<internal::Envelope>();
    auto redirect = env->mutable_request()->mutable_broker_redirect();
    redirect->set_tag(TAG);
    redirect->set_channel(PONG);
    pong_sender.Send(move(env), kBrokerChannel + 1);
  }

  // Send ping message with a tag of the pong machine.
  {
    auto ping_req = MakePing(99);
    ping_sender.Send(*ping_req, configs[0]->MakeMachineId(0, 1), TAG);
  }

  // Now we can the ping message here
  {
    auto ping_req = RecvEnvelope(pong_socket);
    ASSERT_TRUE(ping_req != nullptr);
    ASSERT_TRUE(ping_req->has_request());
    ASSERT_EQ(99, ping_req->request().ping().time());
  }

  // Remove the redirection
  {
    auto env = std::make_unique<internal::Envelope>();
    auto redirect = env->mutable_request()->mutable_broker_redirect();
    redirect->set_tag(TAG);
    redirect->set_stop(true);
    pong_sender.Send(move(env), kBrokerChannel + 1);
  }

  // Send ping message again
  {
    auto ping_req = MakePing(99);
    ping_sender.Send(*ping_req, configs[0]->MakeMachineId(0, 1), TAG);
  }

  // The redirection is removed so we shouldn't be able to receive anything here
  // Theoretically, it is possible that the recv function is called before the
  // pong broker removes the redirection, making the assertion to fail. However,
  // it should be unlikely due to the sleep.
  this_thread::sleep_for(5ms);
  ASSERT_EQ(RecvEnvelope(pong_socket, true), nullptr);
}