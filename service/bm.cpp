#include <sstream>

#include "common/configuration.h"
#include "common/proto_utils.h"
#include "common/service_utils.h"
#include "common/types.h"
#include "benchmark/basic_workload.h"
#include "proto/api.pb.h"

DEFINE_string(config, "slog.conf", "Path to the configuration file");
DEFINE_uint32(replica, 0, "Replica number of the local machine");
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
vector<unique_ptr<zmq::socket_t>> server_sockets;
vector<zmq::pollitem_t> poll_items;
unique_ptr<WorkloadGenerator> workload;
TimePoint start_time;

struct TransactionInfo {
  TransactionType type;
  TimePoint sending_time;
};
uint64_t txn_counter = 0;
unordered_map<uint64_t, TransactionInfo> outstanding_txns;

void InitializeBenchmark();
bool StopConditionMet();

int main(int argc, char* argv[]) {
  InitializeService(argc, argv);
  InitializeBenchmark();

  // Variables to maintain the transaction submission rate
  long wake_up_every_ms = 1000 / FLAGS_rate;
  long poll_timeout_ms = wake_up_every_ms;
  auto wake_up_deadline = Clock::now() + milliseconds(wake_up_every_ms);

  LOG(INFO) << "Start sending transactions";
  start_time = Clock::now();
  while (!StopConditionMet()) {
    if (zmq::poll(poll_items, poll_timeout_ms)) {
      for (size_t i = 0; i < server_sockets.size(); i++) {
        if (poll_items[i].revents & ZMQ_POLLIN) {
          MMessage msg(*server_sockets[i]);
          api::Response res;
          if (!msg.GetProto(res)) {
            LOG(ERROR) << "Malformed response";
          } else {
            if (outstanding_txns.find(res.stream_id()) == outstanding_txns.end()) {
              LOG(ERROR) << "Received response for a non-outstanding txn. Dropping";
            } else {
              // TODO: collect stats here
              // const auto& txn = res.txn().txn();
            }
          }
        }
      }
    } else {
      auto txn = workload->NextTransaction();
      if (FLAGS_print_txn) {
        LOG(INFO) << txn;
      }
      if (!FLAGS_dry_run) {
        api::Request req;
        req.mutable_txn()->mutable_txn()->CopyFrom(txn);
        req.set_stream_id(txn_counter);
        MMessage msg;
        msg.Push(req);
        // TODO: Add an option to randomly send to any server in the same region
        msg.SendTo(*server_sockets[0]);
      }
      txn_counter++;
    }

    // Update poll_timeout_ms to maintain the txn sending rate
    auto now = Clock::now();
    while (now >= wake_up_deadline) {
      wake_up_deadline += milliseconds(wake_up_every_ms);
    }
    auto time_diff = duration_cast<milliseconds>(wake_up_deadline - now);
    poll_timeout_ms = 1 + time_diff.count();
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