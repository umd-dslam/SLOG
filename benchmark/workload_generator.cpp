#include "benchmark/workload_generator.h"

#include <random>

#include <glog/logging.h>

#include "common/mmessage.h"
#include "common/proto_utils.h"
#include "proto/api.pb.h"

namespace slog {

const int NUM_TXN = 100;

WorkloadGenerator::WorkloadGenerator(
    std::shared_ptr<zmq::context_t> context,
    const std::string& host,
    uint32_t port)
  : socket_(*context, ZMQ_DEALER),
    host_(host),
    port_(port) {}

void WorkloadGenerator::SetUp() {
  string endpoint = "tcp://" + host_ + ":" + std::to_string(port_);
  socket_.connect(endpoint);
  LOG(INFO) << "Connected to " << endpoint;
  api::Request req;
  auto txn = MakeTransaction(
      {"read0", "read1"},
      {"write0", "write1"});
  
  req.mutable_txn()
      ->mutable_txn()
      ->CopyFrom(txn);
  for (int i = 0; i < NUM_TXN; i++) {
    req.set_stream_id(i);
    MMessage msg;
    msg.Push(req);
    msg.SendTo(socket_);
    std::this_thread::sleep_for(5ms);
  }
}

void WorkloadGenerator::Loop() {
  MMessage msg(socket_);
  api::Response res;
  if (!msg.GetProto(res)) {
    LOG(ERROR) << "Malformed response";
  } else {
    const auto& txn = res.txn().txn();
    LOG(INFO) << "Received response. Stream id: " << res.stream_id()
              << ". Txn id: " << txn.internal().id();
  }
}

} // namespace slog