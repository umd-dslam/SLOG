#include <sstream>

#include "common/configuration.h"
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
DEFINE_bool(print_txn, false, "Print the each generated transactions");

using std::make_unique;
using std::move;
using std::unique_ptr;
using std::vector;

using namespace slog;

zmq::context_t context(1);
// For controlling rate
unique_ptr<ModuleRunner> ticker;
// Connection
vector<unique_ptr<zmq::socket_t>> server_sockets;
unique_ptr<zmq::socket_t> ticker_socket;
vector<zmq::pollitem_t> poll_items;
// Workload
unique_ptr<WorkloadGenerator> workload;

struct TransactionInfo {
  TransactionType type;
  TimePoint sending_time;
};
TimePoint start_time;
uint64_t txn_counter = 0;
unordered_map<uint64_t, TransactionInfo> outstanding_txns;

void InitializeBenchmark();
bool StopConditionMet();

int main(int argc, char* argv[]) {
  InitializeService(argc, argv);
  InitializeBenchmark();

  LOG(INFO) << "Start sending transactions";
  start_time = Clock::now();
  while (!StopConditionMet()) {
    zmq::poll(poll_items);

    // Check if the ticker ticks
    if (poll_items[0].revents & ZMQ_POLLIN) {
      // Receive the empty message then throw away
      zmq::message_t msg;
      ticker_socket->recv(msg);

      auto txn = workload->NextTransaction();
      if (FLAGS_print_txn) {
        LOG(INFO) << txn << "\nTxn counter: " << txn_counter;
      }
      if (!FLAGS_dry_run) {
        api::Request req;
        req.mutable_txn()->mutable_txn()->CopyFrom(txn);
        req.set_stream_id(txn_counter);
        MMessage msg;
        msg.Push(req);
        // TODO: Add an option to randomly send to any server in the same region
        msg.SendTo(*server_sockets[0]);
        outstanding_txns[txn_counter] = {};
      }
      txn_counter++;
    }
    // Check if we received a response from the server
    for (size_t i = 1; i < poll_items.size(); i++) {
      if (poll_items[i].revents & ZMQ_POLLIN) {
        MMessage msg(*server_sockets[i-1]);
        api::Response res;
        if (!msg.GetProto(res)) {
          LOG(ERROR) << "Malformed response";
        } else {
          if (outstanding_txns.find(res.stream_id()) == outstanding_txns.end()) {
            LOG(ERROR) << "Received response for a non-outstanding txn. Dropping";
          } else {
            // TODO: collect stats here
            const auto& txn = res.txn().txn();
            LOG(INFO) << txn;
          }
        }
      }
    }
  }

  // TODO: collect and print more stats here
  LOG(INFO) << "Elapsed time: " 
            << duration_cast<seconds>(Clock::now() - start_time).count()
            << " seconds";
  return 0;
}

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
    socket->connect(endpoint);
    poll_items.push_back({
        static_cast<void*>(*socket),
        0, /* fd */
        ZMQ_POLLIN,
        0 /* revent */});
    
    server_sockets.push_back(move(socket));
  }

  workload = make_unique<BasicWorkload>(config, FLAGS_data_dir, FLAGS_mh, FLAGS_mp);
}

bool StopConditionMet() {
  if (FLAGS_duration > 0) {
    auto elapsed_time = duration_cast<seconds>(Clock::now() - start_time);
    return elapsed_time.count() >= FLAGS_duration;
  } else if (FLAGS_num_txns > 0) {
    return txn_counter >= FLAGS_num_txns;
  }
  return false;
}