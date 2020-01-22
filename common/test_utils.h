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

using ConfigVec = std::vector<shared_ptr<Configuration>>;

internal::Request MakeEchoRequest(const string& data);
internal::Response MakeEchoResponse(const string& data);

ConfigVec MakeTestConfigurations(
    string&& prefix,
    int num_replicas, 
    int num_partitions,
    uint32_t seed = 0);

using ModuleRunnerPtr = unique_ptr<ModuleRunner>;

class TestSlog {
public:
  TestSlog(shared_ptr<Configuration> config);
  void Data(Key&& key, Record&& record);
  void AddServerAndClient();
  void AddForwarder();
  void AddSequencer();
  void AddScheduler();
  void AddLocalOrderer();

  unique_ptr<Channel> AddChannel(const string& name);

  void StartInNewThreads();
  void SendTxn(const Transaction& txn);

private:
  shared_ptr<Configuration> config_;
  shared_ptr<zmq::context_t> context_;
  shared_ptr<MemOnlyStorage<Key, Record, Metadata>> storage_;
  Broker broker_;
  ModuleRunnerPtr server_;
  ModuleRunnerPtr forwarder_;
  ModuleRunnerPtr sequencer_;
  ModuleRunnerPtr scheduler_;
  ModuleRunnerPtr local_orderer_;

  zmq::context_t client_context_;
  zmq::socket_t client_socket_;
};

} // namespace slog