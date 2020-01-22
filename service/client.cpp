#include <fstream>
#include <zmq.hpp>

#include "common/service_utils.h"
#include "common/proto_utils.h"
#include "proto/api.pb.h"
#include "third_party/rapidjson/document.h"
#include "third_party/rapidjson/istreamwrapper.h"
#include "third_party/rapidjson/writer.h"
#include "third_party/rapidjson/stringbuffer.h"

DEFINE_string(host, "localhost", "Hostname of the SLOG server to connect to");
DEFINE_uint32(port, 5051, "Port number of the SLOG server to connect to");
DEFINE_string(txn_file, "txn.json", "Path to a JSON file containing the transaction");

using namespace slog;
using namespace std;

Transaction* ReadTransactionFromFile(const string& path) {
  ifstream ifs(path, ios_base::in);
  if (!ifs.is_open()) {
    LOG(ERROR) << "Could not open file " << path;
    return nullptr;
  }
  rapidjson::IStreamWrapper json_stream(ifs);
  rapidjson::Document d;
  d.ParseStream(json_stream);
  if (d.HasParseError()) {
    LOG(ERROR) << "Could not parse json in " << path;
    return nullptr;
  }

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  d.Accept(writer);
  LOG(INFO) << "Parsed JSON: " << buffer.GetString();

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

  return new Transaction(
      MakeTransaction(
          read_set,
          write_set,
          d["code"].GetString()));
}

int main(int argc, char* argv[]) {
  InitializeService(argc, argv);

  zmq::context_t context(1);
  zmq::socket_t socket(context, ZMQ_DEALER);
  string endpoint = "tcp://" + FLAGS_host + ":" + to_string(FLAGS_port);
  socket.connect(endpoint);
  LOG(INFO) << "Connected to " << endpoint;

  auto txn = ReadTransactionFromFile(FLAGS_txn_file);
 
  {
    api::Request req;
    req.mutable_txn()->set_allocated_txn(txn);
    MMessage msg;
    msg.Push(req);
    msg.SendTo(socket);
  }
  LOG(INFO) << "Sent a transaction";

  {
    MMessage msg(socket);
    api::Response res;
    if (!msg.GetProto(res)) {
      LOG(ERROR) << "Malformed response";
    } else {
      const auto& txn = res.txn().txn();
      LOG(INFO) << "Received response. Stream id: " << res.stream_id();
      cout << txn;
    }
  }
  return 0;
}