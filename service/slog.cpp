#include <memory>
#include <vector>

#include "common/service_utils.h"
#include "common/configuration.h"
#include "connection/broker.h"
#include "module/consensus.h"
#include "module/forwarder.h"
#include "module/multi_home_orderer.h"
#include "module/scheduler.h"
#include "module/server.h"
#include "module/sequencer.h"
#include "proto/internal.pb.h"
#include "storage/mem_only_storage.h"

DEFINE_string(config, "slog.conf", "Path to the configuration file");
DEFINE_string(address, "", "Address of the local machine");
DEFINE_uint32(replica, 0, "Replica number of the local machine");
DEFINE_uint32(partition, 0, "Partition number of the local machine");

using slog::Storage;
using slog::Broker;
using slog::Key;
using slog::Record;
using slog::Metadata;
using slog::MakeRunnerFor;

using std::make_shared;

int main(int argc, char* argv[]) {
  slog::InitializeService(argc, argv);
  
  auto config = slog::Configuration::FromFile(
      FLAGS_config, 
      FLAGS_address,
      FLAGS_replica,
      FLAGS_partition);
  auto context = make_shared<zmq::context_t>(1);
  Broker broker(config, context);

  auto storage = make_shared<slog::MemOnlyStorage<Key, Record, Metadata>>();
  auto server = MakeRunnerFor<slog::Server>(config, *context, broker, storage);

  vector<unique_ptr<slog::ModuleRunner>> modules;
  modules.push_back(
      MakeRunnerFor<slog::LocalPaxos>(config, broker));
  modules.push_back(
      MakeRunnerFor<slog::Forwarder>(config, broker));
  modules.push_back(
      MakeRunnerFor<slog::Sequencer>(config, broker));
  modules.push_back(
      MakeRunnerFor<slog::Scheduler>(config, *context, broker, storage));
  
  // Only one partition per replica participates in the global paxos process
  if (config->GetLeaderPartitionForMultiHomeOrdering() 
      == config->GetLocalPartition()) {
    modules.push_back(
        MakeRunnerFor<slog::GlobalPaxos>(config, broker));
    modules.push_back(
        MakeRunnerFor<slog::MultiHomeOrderer>(config, broker));
  }

  // New modules cannot be bound to the broker after it starts so only start 
  // the Broker after it is used to initialized all modules.
  broker.StartInNewThread();
  
  // Start modules in the own thread
  for (auto& module : modules) {
    module->StartInNewThread();
  }

  // Run the server in this main thread so that the whole process
  // does not immediately terminate after this line.
  server->Start();

  return 0;
}