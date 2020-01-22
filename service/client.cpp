#include <zmq.hpp>

#include "common/service_utils.h"
#include "common/proto_utils.h"
#include "proto/api.pb.h"

DEFINE_string(host, "localhost", "Hostname of the SLOG server to connect to");
DEFINE_uint32(port, 5051, "Port number of the SLOG server to connect to");

using namespace slog;
using namespace std;

int main(int argc, char* argv[]) {
  InitializeService(argc, argv);

  zmq::context_t context(1);
  zmq::socket_t socket(context, ZMQ_DEALER);

  string endpoint = "tcp://" + FLAGS_host + ":" + to_string(FLAGS_port);
  socket.connect(endpoint);
  LOG(INFO) << "Connected to " << endpoint;

  api::Request req;
  auto txn = MakeTransaction(
      {"A", "B", "C", "D", "E"},
      {"ZZ", "XX", "YY", "VV", "KK"},
      "GET A\n"
      "GET B\n"
      "GET C\n"
      "GET D\n"
      "GET E\n"
      "SET ZZ hello\n"
      "SET XX haha\n"
      "SET YY asasd\n"
      "SET VV asdfsd\n"
      "SET KK asdfasa\n");
  
  req.mutable_txn()->mutable_txn()->CopyFrom(txn);
  {
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