#include <fcntl.h>

#include <memory>
#include <vector>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/monitor.h"
#include "common/offline_data_reader.h"
#include "common/types.h"
#include "connection/broker.h"
#include "module/consensus.h"
#include "module/forwarder.h"
#include "module/interleaver.h"
#include "module/multi_home_orderer.h"
#include "module/scheduler.h"
#include "module/sequencer.h"
#include "module/server.h"
#include "module/ticker.h"
#include "proto/internal.pb.h"
#include "proto/offline_data.pb.h"
#include "service/service_utils.h"
#include "storage/mem_only_storage.h"

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

void LoadData(slog::Storage<Key, Record>& storage, const ConfigurationPtr& config, const string& data_dir) {
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

  VLOG(1) << "First 10 datums are: ";
  int c = 10;
  while (reader.HasNextDatum()) {
    auto datum = reader.GetNextDatum();
    if (c > 0) {
      VLOG(1) << datum.key() << " " << datum.record() << " " << datum.master();
      c--;
    }

    CHECK(config->key_is_in_local_partition(datum.key()))
        << "Key " << datum.key() << " does not belong to partition " << config->local_partition();

    CHECK_LT(datum.master(), config->num_replicas()) << "Master number exceeds number of replicas";

    // Write to storage
    Record record(datum.record(), datum.master());
    storage.Write(datum.key(), record);
  }
  close(fd);
}

void GenerateData(slog::Storage<Key, Record>& storage, const ConfigurationPtr& config) {
  auto simple_partitioning = config->simple_partitioning();
  auto num_records = simple_partitioning->num_records();
  auto num_partitions = config->num_partitions();
  auto partition = config->local_partition();

  // Create a value of specified size by repeating the character 'a'
  string value(simple_partitioning->record_size_bytes(), 'a');

  LOG(INFO) << "Generating ~" << num_records / num_partitions << " records using " << FLAGS_data_threads << " threads. "
            << "Record size = " << simple_partitioning->record_size_bytes() << " bytes";

  std::atomic<uint64_t> counter = 0;
  size_t num_done = 0;
  auto GenerateFn = [&](uint64_t from_key, uint64_t to_key) {
    uint64_t start_key = from_key + (partition + num_partitions - from_key % num_partitions) % num_partitions;
    uint64_t end_key = std::min(to_key, num_records);
    for (uint64_t key = start_key; key < end_key; key += num_partitions) {
      int master = config->master_of_key(key);
      Record record(value, master);
      storage.Write(std::to_string(key), record);
      counter++;
    }
    num_done++;
  };
  std::vector<std::thread> threads;
  uint64_t range = num_records / FLAGS_data_threads + 1;
  for (uint32_t i = 0; i < FLAGS_data_threads; i++) {
    threads.emplace_back(GenerateFn, i * range, (i + 1) * range);
  }
  while (num_done < FLAGS_data_threads) {
    std::this_thread::sleep_for(5s);
    LOG(INFO) << "Generated " << counter.load() << " records";
  }
  for (auto& t : threads) {
    t.join();
  }
}

int main(int argc, char* argv[]) {
  slog::InitializeService(&argc, &argv);

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

  auto config = slog::Configuration::FromFile(FLAGS_config, FLAGS_address);
  const auto& all_addresses = config->all_addresses();
  CHECK(std::find(all_addresses.begin(), all_addresses.end(), config->local_address()) != all_addresses.end())
      << "The configuration does not contain the provided "
      << "local machine ID: \"" << config->local_address() << "\"";
  CHECK_LT(config->local_replica(), config->num_replicas()) << "Replica numbers must be within number of replicas";
  CHECK_LT(config->local_partition(), config->num_partitions())
      << "Partition number must be within number of partitions";

  INIT_TRACING(config);

  if (config->return_dummy_txn()) {
    LOG(WARNING) << "Dummy transactions will be returned";
  }

  auto broker = Broker::New(config);

  // Create and initialize storage layer
  auto storage = make_shared<slog::MemOnlyStorage<Key, Record, Metadata>>();
  // If simple partitioning is used, generate the data;
  // otherwise, load data from an external file
  if (config->simple_partitioning()) {
    GenerateData(*storage, config);
  } else {
    LoadData(*storage, config, FLAGS_data_dir);
  }

  vector<pair<unique_ptr<slog::ModuleRunner>, slog::ModuleId>> modules;
  modules.emplace_back(MakeRunnerFor<slog::Server>(config, broker), slog::ModuleId::SERVER);
  modules.emplace_back(MakeRunnerFor<slog::MultiHomeOrderer>(config, broker), slog::ModuleId::MHORDERER);
  modules.emplace_back(MakeRunnerFor<slog::LocalPaxos>(config, broker), slog::ModuleId::LOCALPAXOS);
  modules.emplace_back(MakeRunnerFor<slog::Forwarder>(config, broker, storage), slog::ModuleId::FORWARDER);
  modules.emplace_back(MakeRunnerFor<slog::Sequencer>(config, broker), slog::ModuleId::SEQUENCER);
  modules.emplace_back(MakeRunnerFor<slog::Interleaver>(config, broker), slog::ModuleId::INTERLEAVER);
  modules.emplace_back(MakeRunnerFor<slog::Scheduler>(config, broker, storage), slog::ModuleId::SCHEDULER);

  // One region is selected to globally order the multihome batches
  if (config->leader_replica_for_multi_home_ordering() == config->local_replica()) {
    modules.emplace_back(MakeRunnerFor<slog::GlobalPaxos>(config, broker), slog::ModuleId::GLOBALPAXOS);
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

  return 0;
}