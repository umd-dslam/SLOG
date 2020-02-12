#pragma once

#include <vector>

#include "common/configuration.h"
#include "connection/broker.h"
#include "module/base/module.h"
#include "storage/mem_only_storage.h"
#include "proto/internal.pb.h"

using std::string;
using std::shared_ptr;
using std::unique_ptr;

namespace slog {

using ConfigVec = std::vector<ConfigurationPtr>;

internal::Request MakeEchoRequest(const string& data);
internal::Response MakeEchoResponse(const string& data);

ConfigVec MakeTestConfigurations(
    string&& prefix,
    int num_replicas, 
    int num_partitions,
    uint32_t seed = 0);

using ModuleRunnerPtr = unique_ptr<ModuleRunner>;

/**
 * This is a fake SLOG system where we can only add a subset
 * of modules to test them in isolation.
 */
class TestSlog {
public:
  TestSlog(ConfigurationPtr config);
  void Data(Key&& key, Record&& record);
  void AddServerAndClient();
  void AddForwarder();
  void AddSequencer();
  void AddScheduler();
  void AddLocalPaxos();
  void AddGlobalPaxos();
  void AddMultiHomeOrderer();

  unique_ptr<Channel> AddChannel(const string& name);

  void StartInNewThreads();
  void SendTxn(const Transaction& txn);
  Transaction RecvTxnResult();

private:
  ConfigurationPtr config_;
  shared_ptr<zmq::context_t> context_;
  shared_ptr<MemOnlyStorage<Key, Record, Metadata>> storage_;
  Broker broker_;
  ModuleRunnerPtr server_;
  ModuleRunnerPtr forwarder_;
  ModuleRunnerPtr sequencer_;
  ModuleRunnerPtr scheduler_;
  ModuleRunnerPtr local_paxos_;
  ModuleRunnerPtr global_paxos_;
  ModuleRunnerPtr multi_home_orderer_;

  zmq::context_t client_context_;
  zmq::socket_t client_socket_;
};

} // namespace slog