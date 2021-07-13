#include <algorithm>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <optional>
#include <random>
#include <unordered_map>

#include "common/configuration.h"
#include "common/csv_writer.h"
#include "common/proto_utils.h"
#include "common/string_utils.h"
#include "module/txn_generator.h"
#include "service/service_utils.h"
#include "workload/basic_workload.h"
#include "workload/remastering_workload.h"
#include "workload/tpcc_workload.h"

DEFINE_string(config, "slog.conf", "Path to the configuration file");
DEFINE_int32(generators, 1, "Number of generator threads");
DEFINE_uint32(r, 0, "The region where the current machine is located");
DEFINE_string(data_dir, "", "Directory containing intial data");
DEFINE_string(out_dir, "", "Directory containing output data");
DEFINE_int32(rate, 0, "Maximum number of transactions sent per second.");
DEFINE_int32(clients, 0, "Number of concurrent client. This option does nothing if 'rate' is set");
DEFINE_int32(duration, 0, "Maximum duration in seconds to run the benchmark");
DEFINE_uint32(txns, 100, "Total number of txns to be generated");
DEFINE_string(wl, "basic", "Name of the workload to use (options: basic, remastering)");
DEFINE_string(params, "", "Parameters of the workload");
DEFINE_bool(dry_run, false, "Generate the transactions without actually sending to the server");
DEFINE_double(sample, 10, "Percent of sampled transactions to be written to result files");
DEFINE_int32(
    seed, -1,
    "Seed for any randomization in the benchmark. If set to negative, seed will be picked from std::random_device()");
DEFINE_bool(txn_profiles, false, "Output transaction profiles");

using namespace slog;

using std::count_if;
using std::make_unique;
using std::setw;
using std::unique_ptr;
using std::chrono::duration_cast;

uint32_t seed = std::random_device{}();
zmq::context_t context;

vector<unique_ptr<ModuleRunner>> InitializeGenerators() {
  // Load the config
  auto config = Configuration::FromFile(FLAGS_config, "");

  // Setup zmq context
  context.set(zmq::ctxopt::blocky, false);

  // Initialize the generators
  FLAGS_generators = std::max(FLAGS_generators, 1);
  auto remaining_txns = FLAGS_txns;
  auto num_txns_per_generator = FLAGS_txns / FLAGS_generators;
  vector<std::unique_ptr<ModuleRunner>> generators;
  for (int i = 0; i < FLAGS_generators; i++) {
    // Select the workload
    unique_ptr<Workload> workload;
    if (FLAGS_wl == "basic") {
      workload = make_unique<BasicWorkload>(config, FLAGS_r, FLAGS_data_dir, FLAGS_params, seed + i);
    } else if (FLAGS_wl == "remastering") {
      workload = make_unique<RemasteringWorkload>(config, FLAGS_r, FLAGS_data_dir, FLAGS_params, seed + i);
    } else if (FLAGS_wl == "tpcc") {
      workload =
          make_unique<TPCCWorkload>(config, FLAGS_r, FLAGS_params, std::make_pair(i + 1, FLAGS_generators), seed + i);
    } else {
      LOG(FATAL) << "Unknown workload: " << FLAGS_wl;
    }
    if (i < FLAGS_generators - 1) {
      remaining_txns -= num_txns_per_generator;
    } else {
      num_txns_per_generator = remaining_txns;
    }
    if (FLAGS_rate > 0) {
      auto tps_per_generator = FLAGS_rate / FLAGS_generators + (i < (FLAGS_rate % FLAGS_generators));
      generators.push_back(MakeRunnerFor<ConstantRateTxnGenerator>(config, context, std::move(workload), FLAGS_r,
                                                                   num_txns_per_generator, tps_per_generator,
                                                                   FLAGS_duration, FLAGS_dry_run));
    } else {
      int num_clients = FLAGS_clients / FLAGS_generators + (i < (FLAGS_clients % FLAGS_generators));
      generators.push_back(MakeRunnerFor<SynchronousTxnGenerator>(config, context, std::move(workload), FLAGS_r,
                                                                  num_txns_per_generator, num_clients, FLAGS_duration,
                                                                  FLAGS_dry_run));
    }
  }
  return generators;
}

void RunBenchmark(vector<unique_ptr<ModuleRunner>>& generators) {
  // Block SIGINT from here so that the new threads inherit the block mask
  sigset_t signal_set;
  sigemptyset(&signal_set);
  sigaddset(&signal_set, SIGINT);
  pthread_sigmask(SIG_BLOCK, &signal_set, nullptr);

  // Run the generators
  for (auto& w : generators) {
    w->StartInNewThread();
  }

  // Wait until all generators finish the setting up phase
  for (;;) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    bool setup = true;
    for (const auto& w : generators) setup &= w->set_up();
    if (setup) break;
  }

  // Status report until all generators finish running
  size_t last_num_sent_txns = 0;
  size_t last_num_recv_txns = 0;
  auto last_print_time = std::chrono::steady_clock::now();
  timespec sigpoll_time = {.tv_sec = 0, .tv_nsec = 0};
  for (;;) {
    std::this_thread::sleep_for(std::chrono::seconds(1));

    bool running = false;
    size_t num_sent_txns = 0;
    size_t num_recv_txns = 0;
    for (auto& w : generators) {
      running |= w->is_running();
      auto gen = dynamic_cast<const TxnGenerator*>(w->module().get());
      num_sent_txns += gen->num_sent_txns();
      num_recv_txns += gen->num_recv_txns();
    }
    auto now = std::chrono::steady_clock::now();
    auto t = duration_cast<std::chrono::milliseconds>(now - last_print_time);
    auto send_tps = (num_sent_txns - last_num_sent_txns) * 1000 / t.count();
    auto recv_tps = (num_recv_txns - last_num_recv_txns) * 1000 / t.count();

    // Effectively skip the first log since it is usually inaccurate.
    if (last_num_sent_txns > 0) {
      LOG(INFO) << "Sent: " << num_sent_txns << "; Received: " << num_recv_txns << "; Sent tps: " << send_tps
                << "; Recv tps: " << recv_tps << "\n";
    }

    last_num_sent_txns = num_sent_txns;
    last_num_recv_txns = num_recv_txns;
    last_print_time = now;

    if (!running) {
      break;
    }

    if (sigtimedwait(&signal_set, nullptr, &sigpoll_time) >= 0) {
      LOG(WARNING) << "Benchmark interuptted. Partial results collected.";
      break;
    }
  }
}

struct ResultWriters {
  const vector<string> kTxnColumns = {"txn_id",    "coordinator",    "replicas", "partitions",
                                      "generator", "global_log_pos", "sent_at",  "received_at"};
  const vector<string> kEventsColumns = {"txn_id", "event", "time", "machine", "home"};
  const vector<string> kSummaryColumns = {"committed",       "aborted",    "not_started",
                                          "single_home",     "multi_home", "single_partition",
                                          "multi_partition", "remaster",   "elapsed_time"};

  ResultWriters()
      : txns(FLAGS_out_dir + "/transactions.csv", kTxnColumns),
        events(FLAGS_out_dir + "/txn_events.csv", kEventsColumns),
        summary(FLAGS_out_dir + "/summary.csv", kSummaryColumns) {}

  CSVWriter txns;
  CSVWriter events;
  CSVWriter summary;
};
std::optional<ResultWriters> writers;

struct GeneratorSummary {
  int committed = 0;
  int aborted = 0;
  int not_started = 0;
  int single_home = 0;
  int multi_home = 0;
  int single_partition = 0;
  int multi_partition = 0;
  int remaster = 0;

  GeneratorSummary& operator+(const GeneratorSummary& other) {
    committed += other.committed;
    aborted += other.aborted;
    not_started += other.not_started;
    single_home += other.single_home;
    multi_home += other.multi_home;
    single_partition += other.single_partition;
    multi_partition += other.multi_partition;
    remaster += other.remaster;
    return *this;
  }

  GeneratorSummary& operator+(const TxnGenerator::TxnInfo& info) {
    committed += info.txn->status() == TransactionStatus::COMMITTED;
    aborted += info.txn->status() == TransactionStatus::ABORTED;
    not_started += info.txn->status() == TransactionStatus::NOT_STARTED;
    single_home += info.txn->internal().type() == TransactionType::SINGLE_HOME;
    multi_home += info.txn->internal().type() == TransactionType::MULTI_HOME_OR_LOCK_ONLY;
    single_partition += info.txn->internal().involved_partitions_size() == 1;
    multi_partition += info.txn->internal().involved_partitions_size() > 1;
    remaster += info.txn->program_case() == Transaction::ProgramCase::kRemaster;
    return *this;
  }

  template <typename T>
  GeneratorSummary& operator+=(const T& other) {
    return *this + other;
  }
};

CSVWriter& operator<<(CSVWriter& csv, const GeneratorSummary& s) {
  csv << s.committed << s.aborted << s.not_started << s.single_home << s.multi_home << s.single_partition
      << s.multi_partition << s.remaster;
  return csv;
}

void WriteMetadata(const Workload& workload) {
  vector<string> columns{"duration", "txns", "clients", "rate", "sample", "wl:name"};
  const auto& param_keys = workload.params().param_keys();
  for (const auto& k : param_keys) {
    columns.push_back("wl:" + k);
  }

  CSVWriter metadata_json(FLAGS_out_dir + "/metadata.csv", columns);
  metadata_json << FLAGS_duration << FLAGS_txns << FLAGS_clients << FLAGS_rate << FLAGS_sample << workload.name();
  for (const auto& k : param_keys) {
    metadata_json << workload.params().GetString(k);
  }
  metadata_json << csvendl;
}

void WriteResults(const vector<unique_ptr<ModuleRunner>>& generators) {
  if (!writers.has_value()) {
    return;
  }

  // Write metadata
  CHECK(!generators.empty());
  auto generator = dynamic_cast<const TxnGenerator*>(generators.front()->module().get());
  WriteMetadata(generator->workload());

  // Aggregate complete data and output summary
  for (auto& w : generators) {
    GeneratorSummary summary;
    auto generator = dynamic_cast<const TxnGenerator*>(w->module().get());
    const auto& txn_infos = generator->txn_infos();
    for (auto info : txn_infos) {
      summary += info;
    }
    writers->summary << summary << generator->elapsed_time().count() << csvendl;
  }

  // Sample a subset of the result
  vector<TxnGenerator::TxnInfo> txn_infos;
  for (auto& w : generators) {
    auto gen = dynamic_cast<const TxnGenerator*>(w->module().get());
    txn_infos.insert(txn_infos.end(), gen->txn_infos().begin(), gen->txn_infos().end());
  }
  std::mt19937 rg(seed);
  std::shuffle(txn_infos.begin(), txn_infos.end(), rg);
  auto sample_size = static_cast<size_t>(txn_infos.size() * FLAGS_sample / 100);
  txn_infos.resize(sample_size);

  for (const auto& info : txn_infos) {
    CHECK(info.txn != nullptr);
    auto& txn_internal = info.txn->internal();
    string involved_replicas, involved_partitions, global_log_pos;
    if (FLAGS_dry_run) {
      involved_replicas = Join(info.profile.involved_replicas());
      involved_partitions = Join(info.profile.involved_partitions());
    } else {
      involved_replicas = Join(txn_internal.involved_replicas());
      involved_partitions = Join(txn_internal.involved_partitions());
      global_log_pos = Join(txn_internal.global_log_positions());
    }
    writers->txns << txn_internal.id() << txn_internal.coordinating_server() << involved_replicas << involved_partitions
                  << info.generator_id << global_log_pos << info.sent_at.time_since_epoch().count()
                  << info.recv_at.time_since_epoch().count() << csvendl;

    for (auto& e : txn_internal.events()) {
      writers->events << txn_internal.id() << ENUM_NAME(e.event(), TransactionEvent) << e.time() << e.machine()
                      << e.home() << csvendl;
    }
  }

  if (FLAGS_txn_profiles) {
    auto file_name = FLAGS_out_dir + "/txn_profiles.txt";
    std::ofstream profiles(file_name, std::ios::out);
    if (!profiles) {
      throw std::runtime_error(std::string("Cannot open file: ") + file_name);
    }
    const int kCellWidth = 12;
    for (const auto& info : txn_infos) {
      profiles << *info.txn;
      profiles << "Multi-Home: " << info.profile.is_multi_home << "\n";
      profiles << "Multi-Partition: " << info.profile.is_multi_partition << "\n";
      profiles << "Profile:\n";
      profiles << setw(kCellWidth) << "Key" << setw(kCellWidth) << "Home" << setw(kCellWidth) << "Partition"
               << setw(kCellWidth) << "Hot" << setw(kCellWidth) << "Write"
               << "\n";
      for (const auto& [key, record] : info.profile.records) {
        profiles << setw(kCellWidth) << key << setw(kCellWidth) << record.home << setw(kCellWidth) << record.partition
                 << setw(kCellWidth) << record.is_hot << setw(kCellWidth) << record.is_write << "\n";
      }
      profiles << "\n" << std::endl;
    }
  }

  LOG(INFO) << "Results were written to \"" << FLAGS_out_dir << "/\"";
}

int main(int argc, char* argv[]) {
  InitializeService(&argc, &argv);

  if (FLAGS_seed >= 0) {
    seed = FLAGS_seed;
  }

  if (FLAGS_dry_run) {
    LOG(WARNING) << "Generating transactions without sending to servers";
  }

  CHECK(FLAGS_clients > 0 || FLAGS_rate > 0) << "Either 'clients' or 'rate' must be set";
  if (FLAGS_clients > 0 && FLAGS_rate > 0) {
    LOG(WARNING) << "The 'rate' flag is set, the 'client' flag will be ignored";
  }

  LOG(INFO) << "Arguments:\n"
            << "Workload: " << FLAGS_wl << "\nParams: " << FLAGS_params << "\nNum txns: " << FLAGS_txns
            << "\nSending rate: " << FLAGS_rate << "\nNum clients: " << FLAGS_clients
            << "\nDuration: " << FLAGS_duration;

  if (FLAGS_out_dir.empty()) {
    LOG(WARNING) << "Results will not be written to files because output directory is not provided";
  } else {
    LOG(INFO) << "Results will be written to \"" << FLAGS_out_dir << "/\"";
    writers.emplace();
  }

  auto generators = InitializeGenerators();

  RunBenchmark(generators);

  WriteResults(generators);

  float avg_tps = 0;
  GeneratorSummary summary;
  for (auto& w : generators) {
    auto generator = dynamic_cast<const TxnGenerator*>(w->module().get());
    const auto& txn_infos = generator->txn_infos();
    int prev_committed = summary.committed;
    for (auto info : txn_infos) {
      summary += info;
    }
    auto elapsed_time = static_cast<float>(generator->elapsed_time().count()) / 1000000000;
    avg_tps += (summary.committed - prev_committed) / elapsed_time;
  }

  LOG(INFO) << "Summary:\n"
            << "Avg. TPS: " << std::floor(avg_tps) << "\nAborted: " << summary.aborted
            << "\nCommitted: " << summary.committed << "\nNot started: " << summary.not_started
            << "\nSingle-home: " << summary.single_home << "\nMulti-home: " << summary.multi_home
            << "\nSingle-partition: " << summary.single_partition << "\nMulti-partition: " << summary.multi_partition
            << "\nRemaster: " << summary.remaster;

  google::protobuf::ShutdownProtobufLibrary();

  return 0;
}