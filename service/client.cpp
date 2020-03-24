#include <fstream>
#include <functional>
#include <iostream>
#include <iomanip>

#include "common/service_utils.h"
#include "common/proto_utils.h"
#include "proto/api.pb.h"
#include "third_party/rapidjson/document.h"
#include "third_party/rapidjson/istreamwrapper.h"
#include "third_party/rapidjson/writer.h"
#include "third_party/rapidjson/stringbuffer.h"

DEFINE_string(host, "localhost", "Hostname of the SLOG server to connect to");
DEFINE_uint32(port, 2023, "Port number of the SLOG server to connect to");
DEFINE_uint32(level, 0, "Level of details for the \"stats\" command");

using namespace slog;
using namespace std;

zmq::context_t context(1);
zmq::socket_t server_socket(context, ZMQ_DEALER);

/***********************************************
                Txn Command
***********************************************/

void ExecuteTxn(const char* txn_file) {
  // 1. Read txn from file
  ifstream ifs(txn_file, ios_base::in);
  if (!ifs.is_open()) {
    LOG(ERROR) << "Could not open file " << txn_file;
    return;
  }
  rapidjson::IStreamWrapper json_stream(ifs);
  rapidjson::Document d;
  d.ParseStream(json_stream);
  if (d.HasParseError()) {
    LOG(ERROR) << "Could not parse json in " << txn_file;
    return;
  }

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  d.Accept(writer);
  LOG(INFO) << "Parsed JSON: " << buffer.GetString();

  // 2. Construct a request
  auto read_set_arr = d["read_set"].GetArray();
  unordered_set<string> read_set;
  for (auto& v : read_set_arr) {
    read_set.insert(v.GetString());
  }

  auto write_set_arr = d["write_set"].GetArray();
  unordered_set<string> write_set;
  for (auto& v : write_set_arr) {
    write_set.insert(v.GetString());
  }

  unordered_map<Key, pair<uint32_t, uint32_t>> metadata;
  if (d.HasMember("metadata")) {
    auto json = d["metadata"].GetObject();
    for (auto& mem : d["metadata"].GetObject()) {
      metadata[mem.name.GetString()] = {mem.value.GetUint(), 0};
    }
  }
  auto txn = MakeTransaction(
      read_set,
      write_set,
      d["code"].GetString(),
      metadata);

  api::Request req;
  req.mutable_txn()->set_allocated_txn(txn);

  // 3. Send to the server
  {
    MMessage msg;
    msg.Push(req);
    msg.SendTo(server_socket);
    LOG(INFO) << "Transaction sent";
  }

  // 4. Wait and print response
  {
    MMessage msg(server_socket);
    api::Response res;
    if (!msg.GetProto(res)) {
      LOG(FATAL) << "Malformed response";
    } else {
      const auto& txn = res.txn().txn();
      cout << txn;
    }
  }
}

/***********************************************
                Stats Command
***********************************************/

struct StatsModule {
  api::StatsModule api_enum;
  function<void(const rapidjson::Document&, uint32_t level)> print_func;
};

void PrintSchedulerStats(const rapidjson::Document& stats, uint32_t level) {
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  stats.Accept(writer);

  cout << "Number of all txns: " << stats[NUM_ALL_TXNS].GetUint() << endl;
  if (level >= 1) {
    cout << "List of all txns:\n  ";
    for (auto& txn_id : stats[ALL_TXNS].GetArray()) {
      cout << txn_id.GetUint() << " ";
    }
    cout << endl;
  }

  cout << "Number of ready workers: " << stats[NUM_READY_WORKERS].GetUint() << endl;
  cout << "Number of ready txns: " << stats[NUM_READY_TXNS].GetUint() << endl;

  cout << "Number of locked keys: " << stats[NUM_LOCKED_KEYS].GetUint() << endl;
  cout << "Number of txns waiting for lock: " << stats[NUM_TXNS_WAITING_FOR_LOCK].GetUint() << endl;
  if (level >= 1) {
    cout << "Number of locks waited per txn: " << endl;
    cout << setw(5) << "Txn" << setw(18) << "# locks waited" << endl;
    for (auto& pair : stats[NUM_LOCKS_WAITED_PER_TXN].GetArray()) {
      cout << setw(5) << pair.GetArray()[0].GetUint() 
           << setw(18) << pair.GetArray()[1].GetUint() << endl;
    }
  }
}

const unordered_map<string, StatsModule> STATS_MODULES = {
  {"scheduler", {api::StatsModule::SCHEDULER, PrintSchedulerStats}}
};

void ExecuteStats(const char* module, uint32_t level) {
  auto& stats_module = STATS_MODULES.at(string(module));

  // 1. Construct a request for stats
  api::Request req;
  req.mutable_stats()->set_module(stats_module.api_enum);
  req.mutable_stats()->set_level(level);

  // 2. Send to the server
  {
    MMessage msg;
    msg.Push(req);
    msg.SendTo(server_socket);
  }

  // 3. Wait and print response
  {
    MMessage msg(server_socket);
    api::Response res;
    if (!msg.GetProto(res)) {
      LOG(FATAL) << "Malformed response";
    } else {
      rapidjson::Document stats;
      stats.Parse(res.stats().stats_json().c_str());
      stats_module.print_func(stats, level);
    }
  }
}

int main(int argc, char* argv[]) {
  slog::InitializeService(&argc, &argv);
  string endpoint = "tcp://" + FLAGS_host + ":" + to_string(FLAGS_port);
  LOG(INFO) << "Connecting to " << endpoint;
  server_socket.connect(endpoint);

  if (argc - 1 == 0) {
    LOG(ERROR) << "Please specify a command";
    return 1;
  }

  if (strcmp(argv[1], "txn") == 0) {
    if (argc - 1 != 2) {
      LOG(ERROR) << "Invalid number of arguments for the \"txn\" command:\n"
                 << "Usage: txn <txn_file>";
      return 1;
    }
    ExecuteTxn(argv[2]);
  } else if (strcmp(argv[1], "stats") == 0) {
    if (argc - 1 != 2) {
      LOG(ERROR) << "Invalid number of arguments for the \"stats\" command:\n"
                 << "Usage: stats <module>";
      return 1;
    }
    ExecuteStats(argv[2], FLAGS_level);
  } else {
    LOG(ERROR) << "Invalid command: " << argv[1];
  }
  return 0;
}