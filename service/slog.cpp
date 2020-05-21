#include <memory>
#include <vector>
#include <fcntl.h>

#include "common/configuration.h"
#include "common/offline_data_reader.h"
#include "common/service_utils.h"
#include "connection/broker.h"
#include "module/consensus.h"
#include "module/forwarder.h"
#include "module/multi_home_orderer.h"
#include "module/scheduler.h"
#include "module/server.h"
#include "module/sequencer.h"
#include "module/ticker.h"
#include "proto/internal.pb.h"
#include "proto/offline_data.pb.h"
#include "storage/mem_only_storage.h"

DEFINE_string(config, "slog.conf", "Path to the configuration file");
DEFINE_string(address, "", "Address of the local machine");
DEFINE_uint32(replica, 0, "Replica number of the local machine");
DEFINE_uint32(partition, 0, "Partition number of the local machine");
DEFINE_string(data_dir, "", "Directory containing intial data");

using slog::Broker;
using slog::ConfigurationPtr;
using slog::Key;
using slog::Record;
using slog::Metadata;
using slog::MakeRunnerFor;

using std::make_shared;

void LoadData(
    slog::Storage<Key, Record>& storage,
    ConfigurationPtr config,
    const string& data_dir) {
  if (data_dir.empty()) {
    LOG(INFO) << "No initial data directory specified. Starting with an empty storage.";
    return;
  }

  auto data_file = data_dir + "/" + 
      std::to_string(config->GetLocalPartition()) + ".dat";

  auto fd = open(data_file.c_str(), O_RDONLY);
  if (fd < 0) {
    LOG(ERROR) << "Error while loading \"" << data_file << "\": "
               << strerror(errno)
               << ". Starting with an empty storage.";
    return;
  }

  slog::OfflineDataReader reader(fd);
  LOG(INFO) << "Loading " << reader.GetNumDatums() << " datums...";

  VLOG(1) << "First 10 datums are: ";
  int c = 10;
  while (reader.HasNextDatum()) {
    auto datum = reader.GetNextDatum();
    if (c > 0) {
      VLOG(1) << datum.key() << " " << datum.record() << " " << datum.master();
      c--;
    }

    CHECK(config->KeyIsInLocalPartition(datum.key()))
        << "Key " << datum.key()
        << " does not belong to partition " << config->GetLocalPartition();

    CHECK_LT(datum.master(), config->GetNumReplicas())
        << "Master number exceeds number of replicas";

    // Write to storage
    Record record(datum.record(), datum.master());
    storage.Write(datum.key(), record);
  }
  close(fd);
}

int main(int argc, char* argv[]) {
  slog::InitializeService(&argc, &argv);
  
  auto zmq_version = zmq::version();
  LOG(INFO) << "ZMQ version "
            << std::get<0>(zmq_version) << "."
            << std::get<1>(zmq_version) << "."
            << std::get<2>(zmq_version);

  auto config = slog::Configuration::FromFile(
      FLAGS_config, 
      FLAGS_address,
      FLAGS_replica,
      FLAGS_partition);
  const auto& all_addresses = config->GetAllAddresses();
  CHECK(std::find(
      all_addresses.begin(),
      all_addresses.end(),
      config->GetLocalAddress()) != all_addresses.end())
      << "The configuration does not contain the provided "
      << "local machine ID: \"" << config->GetLocalAddress() << "\"";
  CHECK_LT(config->GetLocalReplica(), config->GetNumReplicas())
      << "Replica numbers must be within number of replicas";
  CHECK_LT(config->GetLocalPartition(), config->GetNumPartitions())
      << "Partition number must be within number of partitions";

  auto context = make_shared<zmq::context_t>(1);
  Broker broker(config, context);

  // Create and initialize storage layer
  auto storage = make_shared<slog::MemOnlyStorage<Key, Record, Metadata>>();
  LoadData(*storage, config, FLAGS_data_dir);

  // Create the server module. This is not added to the "modules" 
  // list below because it starts differently.
  auto server = MakeRunnerFor<slog::Server>(config, broker, storage);

  vector<unique_ptr<slog::ModuleRunner>> modules;
  modules.push_back(
      MakeRunnerFor<slog::Ticker>(*context, milliseconds(config->GetBatchDuration())));
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

  // New modules cannot be bound to the broker after it starts so start 
  // the Broker only after it is used to initialized all modules.
  broker.StartInNewThread();
  
  // Start modules in their own threads
  for (auto& module : modules) {
    module->StartInNewThread();
  }

  // Run the server in the current main thread so that the whole process
  // does not immediately terminate after this line.
  server->Start();

  return 0;
}