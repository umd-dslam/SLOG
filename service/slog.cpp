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

using namespace slog;
using namespace std;

using std::make_shared;

int main(int argc, char* argv[]) {
  InitializeService(argc, argv);
  
  auto config = Configuration::FromFile(
      FLAGS_config, 
      FLAGS_address,
      FLAGS_replica,
      FLAGS_partition);
  auto context = make_shared<zmq::context_t>(1);
  Broker broker(config, context);

  auto storage = make_shared<MemOnlyStorage<Key, Record, Metadata>>();
  auto server = MakeRunnerFor<Server>(config, *context, broker, storage);

  vector<unique_ptr<ModuleRunner>> modules;
  modules.push_back(
      MakeRunnerFor<LocalPaxos>(config, broker));
  modules.push_back(
      MakeRunnerFor<Forwarder>(config, broker));
  modules.push_back(
      MakeRunnerFor<Sequencer>(config, broker));
  modules.push_back(
      MakeRunnerFor<Scheduler>(config, *context, broker, storage));
  
  if (config->GetLeaderPartitionForMultiHomeOrdering() 
      == config->GetLocalPartition()) {
    modules.push_back(
        MakeRunnerFor<GlobalPaxos>(config, broker));
    modules.push_back(
        MakeRunnerFor<MultiHomeOrderer>(config, broker));
  }

  // Only start the Broker after it is used to initialized all modules
  broker.StartInNewThread();
  for (auto& module : modules) {
    module->StartInNewThread();
  }
  server->Start();

  return 0;
}