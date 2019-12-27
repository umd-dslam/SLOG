#include "common/test_utils.h"

#include <random>

#include "common/proto_utils.h"
#include "proto/api.pb.h"

using std::to_string;

namespace slog {

internal::Request MakeEchoRequest(const std::string& data) {
  internal::Request request;
  auto echo = request.mutable_echo();
  echo->set_data(data);
  return request;
}

internal::Response MakeEchoResponse(const std::string& data) {
  internal::Response response;
  auto echo = response.mutable_echo();
  echo->set_data(data);
  return response;
}

ConfigVec MakeTestConfigurations(
    string&& prefix,
    int num_replicas, 
    int num_partitions) {
  std::random_device rd;
  std::mt19937 re(rd());
  std::uniform_int_distribution<> dis(20000, 30000);
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
      // Generate different server ports because tests 
      // run on the same machine
      common_config.set_server_port(dis(re));
      int i = rep * num_partitions + part;
      string local_addr = addr + to_string(i);
      configs.push_back(std::make_shared<Configuration>(
          common_config,
          local_addr,
          rep, 
          part));
    }
  }
  
  return configs;
}

TestSlog::TestSlog(shared_ptr<Configuration> config)
  : config_(config),
    server_context_(new zmq::context_t(1)),
    storage_(new MemOnlyStorage()),
    broker_(config, server_context_),
    client_context_(1),
    client_socket_(client_context_, ZMQ_DEALER) {}

void TestSlog::Data(Key&& key, Record&& record) {
  storage_->Write(key, record);
}

void TestSlog::AddServerAndClient() {
  server_ = MakeRunnerFor<Server>(
      config_, *server_context_, broker_, storage_);
}

void TestSlog::AddForwarder() {
  forwarder_ = MakeRunnerFor<Forwarder>(config_, broker_);
}

Channel* TestSlog::AddChannel(const string& name) {
  return broker_.AddChannel(name);
}

void TestSlog::StartInNewThreads() {
  broker_.StartInNewThread();
  if (server_) {
    server_->StartInNewThread();
    string endpoint = 
        "tcp://localhost:" + to_string(config_->GetServerPort());
    client_socket_.connect(endpoint);
  }
  if (forwarder_) {
    forwarder_->StartInNewThread();
  }
}

void TestSlog::SendTxn(const Transaction& txn) {
  CHECK(server_ != nullptr) << "TestSlog does not have a server";
  api::Request request;
  auto txn_req = request.mutable_txn();
  txn_req->mutable_txn()->CopyFrom(txn);
  MMessage msg;
  msg.Push(request);
  msg.SendTo(client_socket_);
}

} // namespace slog