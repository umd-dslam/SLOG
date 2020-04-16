#include <sstream>
#include <iostream>
#include <iomanip>

#include "common/configuration.h"
#include "common/csv_writer.h"
#include "common/proto_utils.h"
#include "common/service_utils.h"
#include "common/types.h"
#include "module/ticker.h"
#include "proto/api.pb.h"
#include "workload/basic_workload.h"

DEFINE_string(config, "slog.conf", "Path to the configuration file");
DEFINE_uint32(replica, 0, "The region where the current machine is located");
DEFINE_string(data_dir, "", "Directory containing intial data");
DEFINE_uint32(rate, 1000, "Maximum number of transactions sent per second");
DEFINE_uint32(
    duration,
    0,
    "How long the benchmark is run in seconds. "
    "This is mutually exclusive with \"num_txns\"");
DEFINE_uint32(
    num_txns,
    0,
    "Total number of txns being sent. "
    "This is mutually exclusive with \"duration\"");
DEFINE_double(mh, 0, "Percentage of multi-home transactions");
DEFINE_double(mp, 0, "Percentage of multi-partition transactions");
DEFINE_bool(dry_run, false, "Generate the transactions without actually sending to the server");
DEFINE_bool(print_txn, false, "Print each generated transaction");
DEFINE_bool(print_profile, false, "Print the profile of each transaction");

using namespace slog;

using std::cout;
using std::endl;
using std::fixed;
using std::make_unique;
using std::move;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;

template<typename T>
uint64_t TimeElapsedSince(TimePoint tp) {
  return duration_cast<T>(Clock::now() - tp).count();
}

const uint32_t STATS_PRINT_EVERY_MS = 1000;
const string TXNS_FILE = "transactions.csv";

// Time points are in microseconds since epoch
// Time spans are in microseconds
const vector<string> TXN_COLUMNS = {
    "client_txn_id",
    "txn_id",
    "is_mh",
    "is_mp",
    "sent_at",
    "received_at",
    "rtt"};
const string EVENTS_FILE = "events.csv";
const vector<string> EVENT_COLUMNS = {
    "txn_id",
    "event",
    "time",
    "machine"};

/**
 * Connection stuff
 */
zmq::context_t context(1);
vector<unique_ptr<zmq::socket_t>> server_sockets;
unique_ptr<zmq::socket_t> ticker_socket;
vector<zmq::pollitem_t> poll_items;

/**
 * Used for controlling rate
 * Note: This must be declared after `context` so that
 *       it is destructed before `context`.
 */
unique_ptr<ModuleRunner> ticker;

/**
 * Selected workload
 */
unique_ptr<WorkloadGenerator> workload;

/**
 * Data structure for keeping track of the transactions
 */
struct TransactionInfo {
  TransactionProfile profile;
  TimePoint sent_at;
};
unordered_map<uint64_t, TransactionInfo> outstanding_txns;
std::unique_ptr<CSVWriter> txn_writer;
std::unique_ptr<CSVWriter> event_writer;

/**
 * Statistics
 */
struct Statistics {
  TimePoint start_time;
  uint64_t txn_counter = 0;
  uint32_t resp_counter = 0;

  void MaybePrint() {
    if (TimeElapsedSince<milliseconds>(time_last_print_) < STATS_PRINT_EVERY_MS) {
      return;
    }
    Print();
    time_last_print_ = Clock::now();
  }

  void FinalPrint() {
    Print();
    cout << "Elapsed time: " << TimeElapsedSince<seconds>(start_time)
         << " seconds" << endl;
  }

private:
  TimePoint time_last_print_;

  TimePoint time_last_throughput_;
  uint32_t resp_last_throughput_ = 0;

  double Throughput() {
    double time_since_last_compute_sec =
        TimeElapsedSince<milliseconds>(time_last_throughput_) / 1000.0;
    double throughput = (resp_counter - resp_last_throughput_) / time_since_last_compute_sec;
    time_last_throughput_ = Clock::now();
    resp_last_throughput_ = resp_counter;
    return throughput;
  }

  void Print() {
    cout.precision(1);
    cout << "\nTransactions sent: " << txn_counter << "\n";
    cout << "Responses received: " << resp_counter << "\n";
    cout << "Throughput: " << fixed << Throughput() << " txns/s" << endl;
  }

} stats;

void InitializeBenchmark() {
  if (FLAGS_duration > 0 && FLAGS_num_txns > 0) {
    LOG(FATAL) << "Only either \"duration\" or \"num_txns\" can be set"; 
  }
  
  // Create a ticker and subscribe to it
  ticker = MakeRunnerFor<Ticker>(context, FLAGS_rate);
  ticker->StartInNewThread();
  ticker_socket = make_unique<zmq::socket_t>(context, ZMQ_SUB);
  ticker_socket->connect(Ticker::ENDPOINT);
  ticker_socket->setsockopt(ZMQ_SUBSCRIBE, "", 0);
  // This has to be pushed to poll_items before the server sockets
  poll_items.push_back({
      static_cast<void*>(*ticker_socket),
      0, /* fd */
      ZMQ_POLLIN,
      0 /* revent */});

  ConfigurationPtr config =
      Configuration::FromFile(FLAGS_config, "", FLAGS_replica);

  // Connect to all server in the same region
  for (uint32_t p = 0; p < config->GetNumPartitions(); p++) {
    std::ostringstream endpoint_s;
    if (config->GetProtocol() == "ipc") {
      endpoint_s << "tcp://localhost:"  << config->GetServerPort();
    } else {
      endpoint_s << "tcp://" << config->GetAddress(FLAGS_replica, p) << ":" << config->GetServerPort();
    }
    auto endpoint = endpoint_s.str();

    LOG(INFO) << "Connecting to " << endpoint;
    auto socket = make_unique<zmq::socket_t>(context, ZMQ_DEALER);
    socket->setsockopt(ZMQ_SNDHWM, 0);
    socket->setsockopt(ZMQ_RCVHWM, 0);
    socket->connect(endpoint);
    poll_items.push_back({
        static_cast<void*>(*socket),
        0, /* fd */
        ZMQ_POLLIN,
        0 /* revent */});
    
    server_sockets.push_back(move(socket));
  }

  workload = make_unique<BasicWorkload>(config, FLAGS_data_dir, FLAGS_mh, FLAGS_mp);
  txn_writer = make_unique<CSVWriter>(TXNS_FILE, TXN_COLUMNS);
  event_writer = make_unique<CSVWriter>(EVENTS_FILE, EVENT_COLUMNS);
}

bool StopConditionMet();
void SendNextTransaction();
void ReceiveResult(int from_socket);

void RunBenchmark() {
  LOG(INFO) << "\n"
            << "NUM_RECORDS = " << NUM_RECORDS << "\n"
            << "NUM_WRITES = " << NUM_WRITES << "\n"
            << "VALUE_SIZE = " << VALUE_SIZE << "\n"
            << "MP_NUM_PARTITIONS = " << MP_NUM_PARTITIONS << "\n"
            << "MH_NUM_HOMES = " << MH_NUM_HOMES << "\n";

  stats.start_time = Clock::now();
  while (!StopConditionMet() || !outstanding_txns.empty()) {
    if (zmq::poll(poll_items, 10)) {
      // Check if the ticker ticks
      if (!StopConditionMet() && poll_items[0].revents & ZMQ_POLLIN) {

        // Receive the empty message then throw away
        zmq::message_t msg;
        ticker_socket->recv(msg);

        SendNextTransaction();
      }
      // Check if we received a response from the server
      for (size_t i = 1; i < poll_items.size(); i++) {
        if (poll_items[i].revents & ZMQ_POLLIN) {
          ReceiveResult(i - 1);
        }
      }
    }
    stats.MaybePrint();
  }
  stats.FinalPrint();
}

bool StopConditionMet() {
  if (FLAGS_duration > 0) {
    return TimeElapsedSince<seconds>(stats.start_time) >= FLAGS_duration;
  } else if (FLAGS_num_txns > 0) {
    return stats.txn_counter >= FLAGS_num_txns;
  }
  return false;
}

void SendNextTransaction() {
  auto txn_and_profile = workload->NextTransaction();
  auto txn = txn_and_profile.first;
  auto profile = txn_and_profile.second;

  if (FLAGS_print_txn) {
    LOG(INFO) << *txn;
  }

  if (FLAGS_print_profile) {
    std::ostringstream log;
    log << "partition: ";
    for (const auto& p : profile.key_to_partition) {
      log << std::setw(2) << p.second << " ";
    }
    log << "\n" << std::setw(11) << "home: ";
    for (const auto& p : profile.key_to_home) {
      log << std::setw(2) << p.second << " ";
    }
    LOG(INFO) << "Source of keys:\n" << log.str();
  }

  stats.txn_counter++;

  if (FLAGS_dry_run) {
    return;
  }
  api::Request req;
  req.mutable_txn()->set_allocated_txn(txn);
  req.set_stream_id(stats.txn_counter);
  MMessage msg;
  msg.Push(req);

  // TODO: Add an option to randomly send to any server in the same region
  msg.SendTo(*server_sockets[0]);

  auto& txn_info = outstanding_txns[stats.txn_counter];
  txn_info.sent_at = Clock::now();
  txn_info.profile = profile;
}

void ReceiveResult(int from_socket) {
  MMessage msg(*server_sockets[from_socket]);
  const auto& recv_at = Clock::now();

  api::Response res;

  if (!msg.GetProto(res)) {
    LOG(ERROR) << "Malformed response";
    return;
  }

  if (outstanding_txns.find(res.stream_id()) == outstanding_txns.end()) {
    auto txn_id = res.txn().txn().internal().id();
    LOG(ERROR) << "Received response for a non-outstanding txn "
                << "(stream_id = " << res.stream_id()
                << ", txn_id = " << txn_id << "). Dropping...";
  } 

  stats.resp_counter++;

  // Write txn info to csv file
  const auto& txn = res.txn().txn();
  const auto& txn_internal = txn.internal();
  const auto& txn_info = outstanding_txns[res.stream_id()];
  const auto& sent_at = txn_info.sent_at;

  (*txn_writer) << txn_info.profile.client_txn_id
                << txn_internal.id()
                << txn_info.profile.is_multi_home
                << txn_info.profile.is_multi_partition
                << duration_cast<microseconds>(sent_at.time_since_epoch()).count()
                << duration_cast<microseconds>(recv_at.time_since_epoch()).count()
                << duration_cast<microseconds>(recv_at - sent_at).count()
                << csvendl;

  for (int i = 0; i < txn_internal.events_size(); i++) {
    (*event_writer) << txn_internal.id()
                    << ENUM_NAME(txn_internal.events(i), TransactionEvent)
                    << txn_internal.event_times(i)
                    << txn_internal.event_machines(i)
                    << csvendl;
  }

  outstanding_txns.erase(res.stream_id());
}

int main(int argc, char* argv[]) {
  InitializeService(&argc, &argv);
  
  InitializeBenchmark();
  
  RunBenchmark();
 
  return 0;
}