#include "module/client.h"

#include <random>

#include <glog/logging.h>

#include "proto/api.pb.h"

namespace slog {

const int NUM_TXN = 100;

Client::Client(
    std::shared_ptr<zmq::context_t> context,
    const std::string& host,
    uint32_t port)
  : socket_(*context, ZMQ_DEALER),
    host_(host),
    port_(port) {}

void Client::SetUp() {
  string endpoint = "tcp://" + host_ + ":" + std::to_string(port_);
  socket_.connect(endpoint);
  LOG(INFO) << "Connected to " << endpoint;
  api::Request req;
  auto txn = req.mutable_txn()->mutable_txn();
  txn->mutable_read_set()->insert({string{"read0"}, string{""}});
  txn->mutable_write_set()->insert({string{"write0"}, string{"zzz"}});
  for (int i = 0; i < NUM_TXN; i++) {
    req.set_stream_id(i);
    MMessage msg;
    msg.Push(req);
    msg.SendTo(socket_);
  }
}

void Client::Loop() {
  MMessage msg(socket_);
  api::Response res;
  if (!msg.GetProto(res)) {
    LOG(ERROR) << "Malformed response";
  } else {
    const auto& txn = res.txn().txn();
    LOG(INFO) << "Received response. Stream id: " << res.stream_id()
              << ". Txn id: " << txn.id();
  }
}

} // namespace slog