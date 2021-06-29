#include <fcntl.h>

#include <memory>
#include <vector>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/metrics.h"
#include "common/offline_data_reader.h"
#include "common/sharder.h"
#include "common/types.h"
#include "connection/broker.h"
#include "execution/tpcc/load_tables.h"
#include "execution/tpcc/metadata_initializer.h"
#include "module/consensus.h"
#include "module/forwarder.h"
#include "module/interleaver.h"
#include "module/multi_home_orderer.h"
#include "module/scheduler.h"
#include "module/sequencer.h"
#include "module/server.h"
#include "proto/internal.pb.h"
#include "proto/offline_data.pb.h"
#include "service/service_utils.h"
#include "storage/mem_only_storage.h"
#include "storage/metadata_initializer.h"
#include "version.h"

DEFINE_string(config, "slog.conf", "Path to the configuration file");
DEFINE_string(address, "", "Address of the local machine");
DEFINE_string(data_dir, "", "Directory containing intial data");
DEFINE_uint32(data_threads, 3, "Number of threads used to generate initial data");

using slog::Broker;
using slog::ConfigurationPtr;
using slog::Key;
using slog::MakeRunnerFor;
using slog::Metadata;
using slog::Record;

using std::make_shared;

void LoadData(slog::Storage& storage, const ConfigurationPtr& config, const string& data_dir) {
  if (data_dir.empty()) {
    LOG(INFO) << "No initial data directory specified. Starting with an empty storage.";
    return;
  }

  auto data_file = data_dir + "/" + std::to_string(config->local_partition()) + ".dat";

  auto fd = open(data_file.c_str(), O_RDONLY);
  if (fd < 0) {
    LOG(ERROR) << "Error while loading \"" << data_file << "\": " << strerror(errno)
               << ". Starting with an empty storage.";
    return;
  }

  slog::OfflineDataReader reader(fd);
  LOG(INFO) << "Loading " << reader.GetNumDatums() << " datums...";

  auto sharder = slog::Sharder::MakeSharder(config);

  VLOG(1) << "First 10 datums are: ";
  int c = 10;
  while (reader.HasNextDatum()) {
    auto datum = reader.GetNextDatum();
    if (c > 0) {
      VLOG(1) << datum.key() << " " << datum.record() << " " << datum.master();
      c--;
    }

    CHECK(sharder->is_local_key(datum.key()))
        << "Key " << datum.key() << " does not belong to partition " << config->local_partition();

    CHECK_LT(datum.master(), config->num_replicas()) << "Master number exceeds number of replicas";

    // Write to storage
    Record record(datum.record(), datum.master());
    storage.Write(datum.key(), record);
  }
  close(fd);
}

void GenerateSimpleData(std::shared_ptr<slog::Storage> storage,
                        const std::shared_ptr<slog::MetadataInitializer>& metadata_initializer,
                        const ConfigurationPtr& config) {
  auto simple_partitioning = config->proto_config().simple_partitioning();
  auto num_records = simple_partitioning.num_records();
  auto num_partitions = config->num_partitions();
  auto partition = config->local_partition();

  // Create a value of specified size by repeating the character 'a'
  string value(simple_partitioning.record_size_bytes(), 'a');

  LOG(INFO) << "Generating ~" << num_records / num_partitions << " records using " << FLAGS_data_threads << " threads. "
            << "Record size = " << simple_partitioning.record_size_bytes() << " bytes";

  std::atomic<uint64_t> counter = 0;
  std::atomic<size_t> num_done = 0;
  auto GenerateFn = [&](uint64_t from_key, uint64_t to_key) {
    for (uint64_t key = from_key; key < to_key; key += num_partitions) {
      Record record(value);
      record.SetMetadata(metadata_initializer->Compute(std::to_string(key)));
      storage->Write(std::to_string(key), record);
      counter++;
    }
    num_done++;
  };
  std::vector<std::thread> threads;
  uint64_t range = num_records / FLAGS_data_threads + 1;
  for (uint32_t i = 0; i < FLAGS_data_threads; i++) {
    uint64_t range_start = i * range;
    uint64_t partition_of_range_start = range_start % num_partitions;
    uint64_t distance_to_next_in_partition_key =
        (partition - partition_of_range_start + num_partitions) % num_partitions;
    uint64_t from_key = range_start + distance_to_next_in_partition_key;
    uint64_t to_key = std::min((i + 1) * range, num_records);
    threads.emplace_back(GenerateFn, from_key, to_key);
  }
  while (num_done < FLAGS_data_threads) {
    std::this_thread::sleep_for(std::chrono::seconds(5));
    LOG(INFO) << "Generated " << counter.load() << " records";
  }
  for (auto& t : threads) {
    t.join();
  }
}

void GenerateTPCCData(std::shared_ptr<slog::Storage> storage,
                      const std::shared_ptr<slog::MetadataInitializer>& metadata_initializer,
                      const ConfigurationPtr& config) {
  auto tpcc_partitioning = config->proto_config().tpcc_partitioning();
  auto storage_adapter = std::make_shared<slog::tpcc::KVStorageAdapter>(storage, metadata_initializer);
  slog::tpcc::LoadTables(storage_adapter, tpcc_partitioning.warehouses(), config->num_replicas(),
                         config->num_partitions(), config->local_partition(), FLAGS_data_threads);
}

int main(int argc, char* argv[]) {
  slog::InitializeService(&argc, &argv);

  LOG(INFO) << "SLOG version: " << SLOG_VERSION;
  auto zmq_version = zmq::version();
  LOG(INFO) << "ZMQ version " << std::get<0>(zmq_version) << "." << std::get<1>(zmq_version) << "."
            << std::get<2>(zmq_version);

#ifdef REMASTER_PROTOCOL_SIMPLE
  LOG(INFO) << "Simple remaster protocol";
#elif defined REMASTER_PROTOCOL_PER_KEY
  LOG(INFO) << "Per key remaster protocol";
#elif defined REMASTER_PROTOCOL_COUNTERLESS
  LOG(INFO) << "Counterless remaster protocol";
#else
  LOG(INFO) << "Remastering disabled";
#endif /* REMASTER_PROTOCOL_SIMPLE */

  CHECK(!FLAGS_address.empty()) << "Address must not be empty";
  auto config = slog::Configuration::FromFile(FLAGS_config, FLAGS_address);

  INIT_RECORDING(config);

  LOG(INFO) << "Local replica: " << config->local_replica();
  LOG(INFO) << "Local partition: " << config->local_partition();
  std::ostringstream os;
  for (auto r : config->replication_order()) {
    os << r << " ";
  }
  LOG(INFO) << "Replication order: " << os.str();
  LOG(INFO) << "Execution type: " << ENUM_NAME(config->execution_type(), slog::internal::ExecutionType);
  if (config->return_dummy_txn()) {
    LOG(WARNING) << "Dummy transactions will be returned";
  }

  auto broker = Broker::New(config);

  // Create and initialize storage layer
  auto storage = make_shared<slog::MemOnlyStorage>();
  std::shared_ptr<slog::MetadataInitializer> metadata_initializer;
  switch (config->proto_config().partitioning_case()) {
    case slog::internal::Configuration::kSimplePartitioning:
      metadata_initializer =
          make_shared<slog::SimpleMetadataInitializer>(config->num_replicas(), config->num_partitions());
      GenerateSimpleData(storage, metadata_initializer, config);
      break;
    case slog::internal::Configuration::kTpccPartitioning:
      metadata_initializer =
          make_shared<slog::tpcc::TPCCMetadataInitializer>(config->num_replicas(), config->num_partitions());
      GenerateTPCCData(storage, metadata_initializer, config);
      break;
    default:
      metadata_initializer = make_shared<slog::ConstantMetadataInitializer>(0);
      LoadData(*storage, config, FLAGS_data_dir);
      break;
  }

  auto config_name = FLAGS_config;
  if (auto pos = config_name.rfind('/'); pos != std::string::npos) {
    config_name = config_name.substr(pos + 1);
  }
  auto metrics_manager = make_shared<slog::MetricsRepositoryManager>(config_name, config);

  vector<pair<unique_ptr<slog::ModuleRunner>, slog::ModuleId>> modules;
  // clang-format off
  modules.emplace_back(MakeRunnerFor<slog::Server>(broker, metrics_manager),
                       slog::ModuleId::SERVER);
  modules.emplace_back(MakeRunnerFor<slog::MultiHomeOrderer>(broker, metrics_manager),
                       slog::ModuleId::MHORDERER);
  modules.emplace_back(MakeRunnerFor<slog::LocalPaxos>(broker),
                       slog::ModuleId::LOCALPAXOS);
  modules.emplace_back(MakeRunnerFor<slog::Forwarder>(broker->context(), broker->config(), storage,
                                                      metadata_initializer, metrics_manager),
                       slog::ModuleId::FORWARDER);
  modules.emplace_back(MakeRunnerFor<slog::Sequencer>(broker->context(), broker->config(), metrics_manager),
                       slog::ModuleId::SEQUENCER);
  modules.emplace_back(MakeRunnerFor<slog::Interleaver>(broker, metrics_manager),
                       slog::ModuleId::INTERLEAVER);
  modules.emplace_back(MakeRunnerFor<slog::Scheduler>(broker, storage, metrics_manager),
                       slog::ModuleId::SCHEDULER);
  // clang-format on

  // One region is selected to globally order the multihome batches
  if (config->leader_replica_for_multi_home_ordering() == config->local_replica()) {
    modules.emplace_back(MakeRunnerFor<slog::GlobalPaxos>(broker), slog::ModuleId::GLOBALPAXOS);
  }

  // Block SIGINT from here so that the new threads inherit the block mask
  sigset_t signal_set;
  sigemptyset(&signal_set);
  sigaddset(&signal_set, SIGINT);
  pthread_sigmask(SIG_BLOCK, &signal_set, nullptr);

  // New modules cannot be bound to the broker after it starts so start
  // the Broker only after it is used to initialized all modules above.
  broker->StartInNewThreads();
  for (auto& [module, id] : modules) {
    std::optional<uint32_t> cpu;
    if (auto cpus = config->cpu_pinnings(id); !cpus.empty()) {
      cpu = cpus.front();
    }
    module->StartInNewThread(cpu);
  }

  // Suspense this thread until receiving SIGINT
  int sig;
  sigwait(&signal_set, &sig);

  // Shutdown all threads
  for (auto& module : modules) {
    module.first->Stop();
  }
  broker->Stop();

  metrics_manager->AggregateAndFlushToDisk(".");

  return 0;
}