#include "common/test_utils.h"

#include <random>

#include <glog/logging.h>

#include "common/proto_utils.h"
#include "connection/zmq_utils.h"
#include "module/consensus.h"
#include "module/interleaver.h"
#include "module/forwarder.h"
#include "module/multi_home_orderer.h"
#include "module/server.h"
#include "module/sequencer.h"
#include "module/scheduler.h"
#include "module/ticker.h"
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
    int num_partitions,
    uint32_t seed,
    internal::Configuration common_config) {
  std::mt19937 re(seed);
  std::uniform_int_distribution<> dis(20000, 30000);
  int num_machines = num_replicas * num_partitions;
  string addr = "/tmp/test_" + prefix;

  common_config.set_protocol("ipc");
  common_config.set_broker_port(0);
  common_config.set_num_partitions(num_partitions);
  common_config.mutable_hash_partitioning()->set_partition_key_num_bytes(1);
  common_config.set_batch_duration(1);
  for (int r = 0; r < num_replicas; r++) {
    auto replica = common_config.add_replicas();
    for (int p = 0; p < num_partitions; p++) {
      replica->add_addresses(addr + to_string(r * num_partitions + p));
    }
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

Transaction* FillMetadata(Transaction* txn, uint32_t master, uint32_t counter) {
  auto metadata = txn->mutable_internal()->mutable_master_metadata();
  for (auto& key_value : txn->read_set()) {
    auto& m = (*metadata)[key_value.first];
    m.set_master(master);
    m.set_counter(counter);
  }
  for (auto& key_value : txn->write_set()) {
    auto& m = (*metadata)[key_value.first];
    m.set_master(master);
    m.set_counter(counter);
  }
  return txn;
}

TestSlog::TestSlog(const ConfigurationPtr& config)
  : config_(config),
    context_(new zmq::context_t(1)),
    storage_(new MemOnlyStorage<Key, Record, Metadata>()),
    broker_(new Broker(config, context_, 5)),
    client_context_(1),
    client_socket_(client_context_, ZMQ_DEALER) {
  ticker_ = MakeRunnerFor<Ticker>(*context_, milliseconds(config->GetBatchDuration()));
}

void TestSlog::Data(Key&& key, Record&& record) {
  CHECK(config_->KeyIsInLocalPartition(key)) 
      << "Key \"" << key << "\" belongs to partition " << config_->GetPartitionOfKey(key);
  storage_->Write(key, record);
}

void TestSlog::AddServerAndClient() {
  server_ = MakeRunnerFor<Server>(config_, broker_);
}

void TestSlog::AddForwarder() {
  forwarder_ = MakeRunnerFor<Forwarder>(config_, broker_, storage_);
}

void TestSlog::AddSequencer() {
  sequencer_ = MakeRunnerFor<Sequencer>(config_, broker_);
}

void TestSlog::AddInterleaver() {
  interleaver_ = MakeRunnerFor<Interleaver>(config_, broker_);
}

void TestSlog::AddScheduler() {
  scheduler_ = MakeRunnerFor<Scheduler>(config_, broker_, storage_);
}

void TestSlog::AddLocalPaxos() {
  local_paxos_ = MakeRunnerFor<LocalPaxos>(config_, broker_);
}

void TestSlog::AddGlobalPaxos() {
  global_paxos_ = MakeRunnerFor<GlobalPaxos>(config_, broker_);
}

void TestSlog::AddMultiHomeOrderer() {
  multi_home_orderer_ = MakeRunnerFor<MultiHomeOrderer>(config_, broker_);
}

void TestSlog::AddOutputChannel(Channel channel) {
  broker_->AddChannel(channel);

  zmq::socket_t socket(*context_, ZMQ_PULL);
  socket.setsockopt(ZMQ_LINGER, 0);
  socket.bind("inproc://channel_" + to_string(channel));
  channels_[channel] = std::move(socket);
}

zmq::pollitem_t TestSlog::GetPollItemForChannel(Channel channel) {
  CHECK(channels_.count(channel) > 0) << "Channel " << channel << " does not exist";
  return {
      static_cast<void*>(channels_[channel]),
      0, /* fd */
      ZMQ_POLLIN,
      0 /* revent */};
}

unique_ptr<Sender> TestSlog::GetSender() {
  return std::make_unique<Sender>(broker_);
}

void TestSlog::StartInNewThreads() {
  broker_->StartInNewThread();
  ticker_->StartInNewThread();
  if (server_) {
    server_->StartInNewThread();
    string endpoint = 
        "tcp://localhost:" + to_string(config_->GetServerPort());
    client_socket_.connect(endpoint);
  }
  if (forwarder_) {
    forwarder_->StartInNewThread();
  }
  if (sequencer_) {
    sequencer_->StartInNewThread();
  }
  if (interleaver_) {
    interleaver_->StartInNewThread();
  }
  if (scheduler_) {
    scheduler_->StartInNewThread();
  }
  if (local_paxos_) {
    local_paxos_->StartInNewThread();
  }
  if (global_paxos_) {
    global_paxos_->StartInNewThread();
  }
  if (multi_home_orderer_) {
    multi_home_orderer_->StartInNewThread();
  }
}

void TestSlog::SendTxn(Transaction* txn) {
  CHECK(server_ != nullptr) << "TestSlog does not have a server";
  api::Request request;
  auto txn_req = request.mutable_txn();
  txn_req->set_allocated_txn(txn);
  SendProtoWithEmptyDelimiter(client_socket_, request);
}

Transaction TestSlog::RecvTxnResult() {
  api::Response res;
  if (!ReceiveProtoWithEmptyDelimiter(client_socket_, res)) {
    LOG(FATAL) << "Malformed response to client transaction.";
    return Transaction();
  }
  const auto& txn = res.txn().txn();
  LOG(INFO) << "Received response. Stream id: " << res.stream_id();
  return txn;
}

} // namespace slog
