#include "test/test_utils.h"

#include <glog/logging.h>

#include <random>

#include "common/proto_utils.h"
#include "connection/zmq_utils.h"
#include "module/consensus.h"
#include "module/forwarder.h"
#include "module/interleaver.h"
#include "module/multi_home_orderer.h"
#include "module/scheduler.h"
#include "module/sequencer.h"
#include "module/server.h"
#include "module/ticker.h"
#include "proto/api.pb.h"

using std::to_string;

namespace slog {

ConfigVec MakeTestConfigurations(string&& prefix, int num_replicas, int num_partitions,
                                 internal::Configuration common_config) {
  std::random_device rd;
  std::mt19937 re(rd());
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
      configs.push_back(std::make_shared<Configuration>(common_config, local_addr, rep, part));
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

TxnHolder MakeTxnHolder(const ConfigurationPtr& config, TxnId id, const unordered_set<Key>& read_set,
                        const unordered_set<Key>& write_set,
                        const unordered_map<Key, pair<uint32_t, uint32_t>>& master_metadata) {
  Transaction* txn;
  if (master_metadata.empty()) {
    // Some tests need to read the metadata. While it does not matter to those tests, they throw
    // errors if the metadata does not exist
    txn = FillMetadata(MakeTransaction(read_set, write_set, ""), 0, 0);
  } else {
    txn = MakeTransaction(read_set, write_set, "", master_metadata);
  }
  txn->mutable_internal()->set_id(id);
  return {config, txn};
}

TestSlog::TestSlog(const ConfigurationPtr& config)
    : config_(config),
      context_(new zmq::context_t(1)),
      storage_(new MemOnlyStorage<Key, Record, Metadata>()),
      broker_(new Broker(config, context_, kTestModuleTimeout)),
      client_context_(1),
      client_socket_(client_context_, ZMQ_DEALER) {
  context_->set(zmq::ctxopt::blocky, false);
  client_context_.set(zmq::ctxopt::blocky, false);
  ticker_ = MakeRunnerFor<Ticker>(*context_, milliseconds(config->batch_duration()));
}

void TestSlog::Data(Key&& key, Record&& record) {
  CHECK(config_->key_is_in_local_partition(key))
      << "Key \"" << key << "\" belongs to partition " << config_->partition_of_key(key);
  storage_->Write(key, record);
}

void TestSlog::AddServerAndClient() { server_ = MakeRunnerFor<Server>(config_, broker_, kTestModuleTimeout); }

void TestSlog::AddForwarder() { forwarder_ = MakeRunnerFor<Forwarder>(config_, broker_, storage_, kTestModuleTimeout); }

void TestSlog::AddSequencer() { sequencer_ = MakeRunnerFor<Sequencer>(config_, broker_, kTestModuleTimeout); }

void TestSlog::AddInterleaver() { interleaver_ = MakeRunnerFor<Interleaver>(config_, broker_, kTestModuleTimeout); }

void TestSlog::AddScheduler() { scheduler_ = MakeRunnerFor<Scheduler>(config_, broker_, storage_, kTestModuleTimeout); }

void TestSlog::AddLocalPaxos() { local_paxos_ = MakeRunnerFor<LocalPaxos>(config_, broker_, kTestModuleTimeout); }

void TestSlog::AddGlobalPaxos() { global_paxos_ = MakeRunnerFor<GlobalPaxos>(config_, broker_, kTestModuleTimeout); }

void TestSlog::AddMultiHomeOrderer() {
  multi_home_orderer_ = MakeRunnerFor<MultiHomeOrderer>(config_, broker_, kTestModuleTimeout);
}

void TestSlog::AddOutputChannel(Channel channel) {
  broker_->AddChannel(channel);

  zmq::socket_t socket(*context_, ZMQ_PULL);
  socket.bind("inproc://channel_" + to_string(channel));
  channels_.insert_or_assign(channel, std::move(socket));
}

zmq::pollitem_t TestSlog::GetPollItemForChannel(Channel channel) {
  auto it = channels_.find(channel);
  if (it == channels_.end()) {
    LOG(FATAL) << "Channel " << channel << " does not exist";
  }
  return {static_cast<void*>(it->second), 0, /* fd */
          ZMQ_POLLIN, 0 /* revent */};
}

unique_ptr<Sender> TestSlog::GetSender() { return std::make_unique<Sender>(broker_); }

void TestSlog::StartInNewThreads() {
  broker_->StartInNewThread();
  ticker_->StartInNewThread();
  if (server_) {
    server_->StartInNewThread();
    string endpoint = "tcp://localhost:" + to_string(config_->server_port());
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
  SendSerializedProtoWithEmptyDelim(client_socket_, request);
}

Transaction TestSlog::RecvTxnResult() {
  api::Response res;
  if (!RecvDeserializedProtoWithEmptyDelim(client_socket_, res)) {
    LOG(FATAL) << "Malformed response to client transaction.";
    return Transaction();
  }
  const auto& txn = res.txn().txn();
  LOG(INFO) << "Received response. Stream id: " << res.stream_id();
  return txn;
}

}  // namespace slog
