#include <memory>
#include <vector>

#include "common/proto_utils.h"
#include "common/service_utils.h"
#include "common/configuration.h"
#include "connection/broker.h"
#include "module/forwarder.h"
#include "module/orderer.h"
#include "module/scheduler.h"
#include "module/server.h"
#include "module/sequencer.h"
#include "proto/internal.pb.h"
#include "storage/mem_only_storage.h"

DEFINE_string(config, "slog.conf", "Path to the configuration file");
DEFINE_string(address, "", "Address of the local machine");
DEFINE_uint32(replica, 0, "Replica number of the local machine");
DEFINE_uint32(partition, 0, "Partition number of the local machine");

using namespace slog;
using namespace std;

using std::make_shared;

unique_ptr<ModuleRunner> MakeMultihomeOrderer(
    shared_ptr<Configuration> config, Broker& broker) {
  auto local_rep = config->GetLocalReplica();
  auto part = config->GetGlobalPaxosMemberPartition();
  vector<string> members;
  for (uint32_t rep = 0; rep < config->GetNumReplicas(); rep++) {
    members.push_back(
        MakeMachineId(rep, part));
  }
  return MakeRunnerFor<LocalOrderer>(
    broker, members, MakeMachineId(local_rep, part));
}

unique_ptr<ModuleRunner> MakeLocalOrderer(
    shared_ptr<Configuration> config, Broker& broker) {
  auto local_rep = config->GetLocalReplica();
  auto local_part = config->GetLocalPartition();
  vector<string> members;
  // Enlist all machines in the same replica as members
  for (uint32_t part = 0; part < config->GetNumPartitions(); part++) {
    members.push_back(MakeMachineId(local_rep, part));
  }
  return MakeRunnerFor<LocalOrderer>(
      broker, members, MakeMachineId(local_rep, local_part));
}

int main(int argc, char* argv[]) {
  InitializeService(argc, argv);
  
  auto config = Configuration::FromFile(
      FLAGS_config, 
      FLAGS_address,
      FLAGS_replica,
      FLAGS_partition);
  auto context = make_shared<zmq::context_t>(1);
  Broker broker(config, context);

  auto storage = make_shared<MemOnlyStorage>();
  auto server = MakeRunnerFor<Server>(config, *context, broker, storage);

  vector<unique_ptr<ModuleRunner>> modules;
  modules.push_back(
      MakeMultihomeOrderer(config, broker));
  modules.push_back(
      MakeLocalOrderer(config, broker));
  modules.push_back(
      MakeRunnerFor<Forwarder>(config, broker));
  modules.push_back(
      MakeRunnerFor<Sequencer>(config, broker));
  modules.push_back(
      MakeRunnerFor<Scheduler>(config, broker));

  // Only start the Broker after it is used to initialized all modules
  broker.StartInNewThread();
  for (auto& module : modules) {
    module->StartInNewThread();
  }
  server->Start();

  return 0;
}