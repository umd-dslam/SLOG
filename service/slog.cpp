#include <memory>
#include <vector>
#include <fcntl.h>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

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
#include "proto/offline_data.pb.h"
#include "storage/mem_only_storage.h"

DEFINE_string(config, "slog.conf", "Path to the configuration file");
DEFINE_string(address, "", "Address of the local machine");
DEFINE_uint32(replica, 0, "Replica number of the local machine");
DEFINE_uint32(partition, 0, "Partition number of the local machine");
DEFINE_string(data_dir, "", "Directory containing intial data");

using google::protobuf::io::ZeroCopyInputStream;
using google::protobuf::io::FileInputStream;
using google::protobuf::io::CodedInputStream;

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
    LOG(FATAL) << "Data loading error: " << strerror(errno);
  }

  ZeroCopyInputStream* raw_input(new FileInputStream(fd));
  CodedInputStream* coded_input(new CodedInputStream(raw_input));

  uint32_t num_datums;
  if (!coded_input->ReadVarint32(&num_datums)) {
    LOG(FATAL) << "Error while reading data file";
  }
  LOG(INFO) << "Loading " << num_datums << " datums...";

  VLOG(1) << "First 10 datums are: ";
  for (uint32_t i = 0; i < num_datums; i++) {
    int sz;
    // Read the size of the next datum
    if (!coded_input->ReadVarintSizeAsInt(&sz)) {
      LOG(FATAL) << "Error while reading data file";
    }
    string buf;
    // Read the datum given the size
    if (!coded_input->ReadString(&buf, sz)) {
      LOG(FATAL) << "Error while reading data file";
    }

    slog::Datum datum;
    // Parse raw bytes into protobuf object
    datum.ParseFromString(buf);

    if (i < 10) {
      VLOG(1) << datum.key() << " " << datum.record() << " " << datum.master();
    }

    // Write to storage
    Record record(datum.record(), datum.master());
    storage.Write(datum.key(), record);
  }

  delete coded_input;
  delete raw_input;
  close(fd);
}

int main(int argc, char* argv[]) {
  slog::InitializeService(argc, argv);
  
  auto config = slog::Configuration::FromFile(
      FLAGS_config, 
      FLAGS_address,
      FLAGS_replica,
      FLAGS_partition);
  auto context = make_shared<zmq::context_t>(1);
  Broker broker(config, context);

  // Create and initialize storage layer
  auto storage = make_shared<slog::MemOnlyStorage<Key, Record, Metadata>>();
  LoadData(*storage, config, FLAGS_data_dir);

  // Create the server module. This is not added to the "modules" 
  // list below because it starts differently.
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