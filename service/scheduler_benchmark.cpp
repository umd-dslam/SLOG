#include <chrono>

#include "common/configuration.h"
#include "common/csv_writer.h"
#include "common/string_utils.h"
#include "connection/broker.h"
#include "module/scheduler.h"
#include "service/service_utils.h"
#include "storage/mem_only_storage.h"
#include "workload/basic_workload.h"

DEFINE_uint32(txns, 100, "Number of transactions");
DEFINE_uint32(workers, 3, "Number of workers");
DEFINE_uint32(records, 100000, "Number of records");
DEFINE_uint32(record_size, 100, "Size of a record in bytes");
DEFINE_string(params, "hot=0,hot_records=0", "Basic workload params");
DEFINE_double(sample, 10, "Percent of sampled transactions to be written to result files");
DEFINE_string(out_dir, ".", "Directory containing output data");
DEFINE_string(execution, "key_value", "Execution type. Choose from (noop and key_value)");

using namespace slog;
using namespace std::chrono;

using std::make_shared;
using std::string;
using std::vector;

struct TxnInfo {
  using TimePoint = std::chrono::system_clock::time_point;
  Transaction* txn;
  TimePoint sent_at;
};

int main(int argc, char* argv[]) {
  InitializeService(&argc, &argv);

  string address("/tmp/test_scheduler");

  internal::Configuration config_proto;
  config_proto.set_protocol("ipc");
  config_proto.add_broker_ports(0);
  config_proto.set_server_port(5000);
  config_proto.set_sequencer_port(5001);
  config_proto.set_forwarder_port(5002);
  config_proto.set_num_partitions(1);
  config_proto.mutable_simple_partitioning()->set_num_records(FLAGS_records);
  config_proto.mutable_simple_partitioning()->set_record_size_bytes(FLAGS_record_size);
  config_proto.add_replicas()->add_addresses(address);
  config_proto.set_num_workers(FLAGS_workers);
  if (FLAGS_execution == "noop") {
    config_proto.set_execution_type(internal::ExecutionType::NOOP);
  } else if (FLAGS_execution == "key_value") {
    config_proto.set_execution_type(internal::ExecutionType::KEY_VALUE);
  } else {
    LOG(FATAL) << "Unknown commands type: " << FLAGS_execution;
  }

  auto config = make_shared<Configuration>(config_proto, address);
  auto storage = make_shared<slog::MemOnlyStorage>();

  // Prepare the modules
  auto broker = Broker::New(config);
  broker->AddChannel(kServerChannel);
  auto scheduler = MakeRunnerFor<Scheduler>(broker, storage, nullptr);

  broker->StartInNewThreads();
  scheduler->StartInNewThread();

  // Prepare the workload
  BasicWorkload workload(config, 0, "", FLAGS_params);
  vector<Transaction*> transactions;
  LOG(INFO) << "Generating " << FLAGS_txns << " transactions";
  for (size_t i = 0; i < FLAGS_txns; i++) {
    auto txn = workload.NextTransaction().first;
    txn->mutable_internal()->add_involved_replicas(0);
    txn->mutable_internal()->add_involved_partitions(0);
    txn->mutable_internal()->set_coordinating_server(0);
    transactions.push_back(txn);
  }

  // Prepare the socket that receives the results of the txns
  zmq::socket_t result_socket(*broker->context(), ZMQ_PULL);
  result_socket.bind(MakeInProcChannelAddress(kServerChannel));

  auto start_time = std::chrono::steady_clock::now();

  // Send transactions to the scheduler
  LOG(INFO) << "Sending all transactions through the scheduler";
  std::unordered_map<TxnId, TxnInfo::TimePoint> sent_at;
  Sender sender(config, broker->context());
  for (auto txn : transactions) {
    auto env = std::make_unique<internal::Envelope>();
    env->mutable_request()->mutable_forward_txn()->set_allocated_txn(txn);
    sender.Send(std::move(env), kSchedulerChannel);
    sent_at[txn->internal().id()] = std::chrono::system_clock::now();
  }

  // Receive the results
  LOG(INFO) << "Collecting results";
  vector<TxnInfo> results;
  for (size_t i = 0; i < transactions.size(); i++) {
    auto env = RecvEnvelope(result_socket);
    auto txn = env->mutable_request()->mutable_finished_subtxn()->release_txn();
    auto txn_id = txn->internal().id();
    results.push_back({.txn = txn, .sent_at = sent_at[txn_id]});
  }

  auto duration = duration_cast<milliseconds>(std::chrono::steady_clock::now() - start_time);
  LOG(INFO) << "Elapsed time: " << duration.count() / 1000.0 << " s";
  if (duration.count() == 0) {
    LOG(INFO) << "Avg. Throughput: inf txn/s";
  } else {
    auto avg_throughput = FLAGS_txns / (duration.count() / 1000.0);
    LOG(INFO) << "Avg. Throughput: " << std::fixed << std::setprecision(3) << avg_throughput << " txn/s";
  }

  // Sample a subset of the result
  std::mt19937 rg(0);
  std::shuffle(results.begin(), results.end(), rg);
  auto sample_size = static_cast<size_t>(results.size() * FLAGS_sample / 100);
  results.resize(sample_size);

  // Output the results
  const vector<string> kTxnColumns = {"txn_id", "sent_at", "reads", "writes"};
  const vector<string> kEventsColumns = {"txn_id", "event", "time", "machine", "home"};
  CSVWriter profiles(FLAGS_out_dir + "/transactions.csv", kTxnColumns);
  CSVWriter events(FLAGS_out_dir + "/events.csv", kEventsColumns);

  for (const auto& info : results) {
    auto txn = info.txn;
    auto& txn_internal = txn->internal();
    vector<string> reads, writes;
    for (const auto& kv : txn->keys()) {
      const auto& k = kv.key();
      const auto& v = kv.value_entry();
      if (v.type() == KeyType::READ) {
        reads.push_back(k);
      } else {
        writes.push_back(k);
      }
    }
    profiles << txn_internal.id() << info.sent_at.time_since_epoch().count() << Join(reads) << Join(writes) << csvendl;
    for (auto& e : txn_internal.events()) {
      events << txn_internal.id() << ENUM_NAME(e.event(), TransactionEvent) << e.time() << e.machine() << e.home()
             << csvendl;
    }
  }
}