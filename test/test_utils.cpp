#include "test/test_utils.h"

#include <glog/logging.h>

#include <random>
#include <set>

#include "common/proto_utils.h"
#include "connection/zmq_utils.h"
#include "module/consensus.h"
#include "module/forwarder.h"
#include "module/interleaver.h"
#include "module/multi_home_orderer.h"
#include "module/scheduler.h"
#include "module/sequencer.h"
#include "module/server.h"
#include "proto/api.pb.h"

using std::make_shared;
using std::to_string;

namespace slog {

namespace {

std::set<uint32_t> used_ports;
std::mt19937 rng(std::random_device{}());

uint32_t NextUnusedPort() {
  std::uniform_int_distribution<> dis(10000, 30000);
  uint32_t port;
  do {
    port = dis(rng);
  } while (used_ports.find(port) != used_ports.end());
  used_ports.insert(port);
  return port;
}

}  // namespace

ConfigVec MakeTestConfigurations(string&& prefix, int num_replicas, int num_partitions,
                                 internal::Configuration common_config) {
  int num_machines = num_replicas * num_partitions;
  string addr = "/tmp/test_" + prefix;

  common_config.set_protocol("ipc");
  common_config.add_broker_ports(0);
  common_config.add_broker_ports(1);
  common_config.set_forwarder_port(2);
  common_config.set_sequencer_port(3);
  common_config.set_num_partitions(num_partitions);
  common_config.mutable_hash_partitioning()->set_partition_key_num_bytes(1);
  common_config.set_sequencer_batch_duration(1);
  common_config.set_forwarder_batch_duration(1);
  common_config.set_execution_type(internal::ExecutionType::KEY_VALUE);
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
      common_config.set_server_port(NextUnusedPort());
      int i = rep * num_partitions + part;
      string local_addr = addr + to_string(i);
      configs.push_back(std::make_shared<Configuration>(common_config, local_addr));
    }
  }

  return configs;
}

Transaction* MakeTestTransaction(const ConfigurationPtr& config, TxnId id, const std::vector<KeyMetadata>& keys,
                                 const std::vector<std::vector<std::string>> code, std::optional<int> remaster,
                                 MachineId coordinator) {
  auto txn = MakeTransaction(keys, code, remaster, coordinator);
  txn->mutable_internal()->set_id(id);

  PopulateInvolvedPartitions(Sharder::MakeSharder(config), *txn);

  return txn;
}

TxnHolder MakeTestTxnHolder(const ConfigurationPtr& config, TxnId id, const std::vector<KeyMetadata>& keys,
                            const std::vector<std::vector<std::string>> code, std::optional<int> remaster) {
  auto txn = MakeTestTransaction(config, id, keys, code, remaster);

  auto sharder = Sharder::MakeSharder(config);
  vector<Transaction*> lo_txns;
  for (int i = 0; i < txn->internal().involved_replicas_size(); ++i) {
    auto lo = GenerateLockOnlyTxn(txn, txn->internal().involved_replicas(i));
    auto partitioned_lo = GeneratePartitionedTxn(sharder, lo, sharder->local_partition(), true);
    if (partitioned_lo != nullptr) {
      lo_txns.push_back(partitioned_lo);
    }
  }
  delete txn;

  CHECK(!lo_txns.empty());

  TxnHolder holder(config, lo_txns[0]);
  for (size_t i = 1; i < lo_txns.size(); ++i) {
    holder.AddLockOnlyTxn(lo_txns[i]);
  }
  return holder;
}

ValueEntry TxnValueEntry(const Transaction& txn, const std::string& key) {
  auto it = std::find_if(txn.keys().begin(), txn.keys().end(),
                         [&key](const KeyValueEntry& entry) { return entry.key() == key; });
  return it->value_entry();
}

TestSlog::TestSlog(const ConfigurationPtr& config)
    : config_(config),
      sharder_(Sharder::MakeSharder(config)),
      storage_(new MemOnlyStorage()),
      broker_(Broker::New(config, kTestModuleTimeout)),
      client_context_(1) {
  client_context_.set(zmq::ctxopt::blocky, false);
  client_socket_ = zmq::socket_t(client_context_, ZMQ_DEALER);
}

void TestSlog::Data(Key&& key, Record&& record) {
  CHECK(sharder_->is_local_key(key)) << "Key \"" << key << "\" belongs to partition "
                                     << sharder_->compute_partition(key);
  storage_->Write(key, record);
}

void TestSlog::AddServerAndClient() { server_ = MakeRunnerFor<Server>(broker_, nullptr, kTestModuleTimeout); }

void TestSlog::AddForwarder() {
  metadata_initializer_ = std::make_shared<ConstantMetadataInitializer>(0);
  forwarder_ = MakeRunnerFor<Forwarder>(broker_->context(), broker_->config(), storage_, metadata_initializer_, nullptr,
                                        kTestModuleTimeout);
}

void TestSlog::AddSequencer() {
  sequencer_ = MakeRunnerFor<Sequencer>(broker_->context(), broker_->config(), nullptr, kTestModuleTimeout);
}

void TestSlog::AddInterleaver() { interleaver_ = MakeRunnerFor<Interleaver>(broker_, nullptr, kTestModuleTimeout); }

void TestSlog::AddScheduler() { scheduler_ = MakeRunnerFor<Scheduler>(broker_, storage_, nullptr, kTestModuleTimeout); }

void TestSlog::AddLocalPaxos() { local_paxos_ = MakeRunnerFor<LocalPaxos>(broker_, kTestModuleTimeout); }

void TestSlog::AddGlobalPaxos() { global_paxos_ = MakeRunnerFor<GlobalPaxos>(broker_, kTestModuleTimeout); }

void TestSlog::AddMultiHomeOrderer() {
  multi_home_orderer_ = MakeRunnerFor<MultiHomeOrderer>(broker_, nullptr, kTestModuleTimeout);
}

void TestSlog::AddOutputSocket(Channel channel) {
  switch (channel) {
    case kForwarderChannel: {
      zmq::socket_t outproc_socket(*broker_->context(), ZMQ_PULL);
      outproc_socket.bind(
          MakeRemoteAddress(config_->protocol(), config_->local_address(), config_->forwarder_port(), true));
      outproc_sockets_[channel] = std::move(outproc_socket);
      break;
    }
    case kSequencerChannel: {
      zmq::socket_t outproc_socket(*broker_->context(), ZMQ_PULL);
      outproc_socket.bind(
          MakeRemoteAddress(config_->protocol(), config_->local_address(), config_->sequencer_port(), true));
      outproc_sockets_[channel] = std::move(outproc_socket);
      break;
    }
    default:
      broker_->AddChannel(channel);
  }

  zmq::socket_t inproc_socket(*broker_->context(), ZMQ_PULL);
  inproc_socket.bind(MakeInProcChannelAddress(channel));
  inproc_sockets_.insert_or_assign(channel, std::move(inproc_socket));
}

zmq::pollitem_t TestSlog::GetPollItemForOutputSocket(Channel channel, bool inproc) {
  if (inproc) {
    auto it = inproc_sockets_.find(channel);
    CHECK(it != inproc_sockets_.end()) << "Inproc socket " << channel << " does not exist";
    return {static_cast<void*>(it->second), 0, /* fd */
            ZMQ_POLLIN, 0 /* revent */};
  }
  auto it = outproc_sockets_.find(channel);
  CHECK(it != outproc_sockets_.end()) << "Outproc socket " << channel << " does not exist";
  return {static_cast<void*>(it->second), 0, /* fd */
          ZMQ_POLLIN, 0 /* revent */};
}

EnvelopePtr TestSlog::ReceiveFromOutputSocket(Channel channel, bool inproc) {
  if (inproc) {
    CHECK(inproc_sockets_.count(channel) > 0) << "Inproc socket \"" << channel << "\" does not exist";
    return RecvEnvelope(inproc_sockets_[channel]);
  }
  CHECK(outproc_sockets_.count(channel) > 0) << "Outproc socket \"" << channel << "\" does not exist";
  zmq::message_t msg;
  outproc_sockets_[channel].recv(msg);
  return DeserializeEnvelope(msg);
}

unique_ptr<Sender> TestSlog::NewSender() { return std::make_unique<Sender>(broker_->config(), broker_->context()); }

void TestSlog::StartInNewThreads() {
  broker_->StartInNewThreads();
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
