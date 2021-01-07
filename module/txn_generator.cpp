#include "module/txn_generator.h"

#include <glog/logging.h>

#include <sstream>

#include "common/constants.h"
#include "connection/zmq_utils.h"
#include "proto/api.pb.h"

using std::shared_ptr;
using std::unique_ptr;
using std::chrono::system_clock;

namespace slog {

const uint32_t kOverheadEstimate = 20;

TxnGenerator::TxnGenerator(const ConfigurationPtr& config, zmq::context_t& context, unique_ptr<Workload>&& workload,
                           uint32_t region, uint32_t num_txns, uint32_t tps, bool dry_run)
    : Module("Txn-Generator"),
      config_(config),
      socket_(context, ZMQ_DEALER),
      workload_(std::move(workload)),
      poller_(kModuleTimeout),
      region_(region),
      num_txns_(num_txns),
      dry_run_(dry_run),
      cur_txn_(0),
      num_recv_txns_(0) {
  CHECK(workload_ != nullptr) << "Must provide a valid workload";

  CHECK_LT(tps, 1000000) << "Transaction/sec is too high (max. 1000000)";
  if (1000000 / tps > kOverheadEstimate) {
    interval_ = std::chrono::microseconds(1000000 / tps - kOverheadEstimate);
  } else {
    interval_ = 0us;
  }
}

void TxnGenerator::SetUp() {
  LOG(INFO) << "Generating " << num_txns_ << " transactions";
  for (size_t i = 0; i < num_txns_; i++) {
    auto new_txn = workload_->NextTransaction();
    TxnInfo info{
        .txn = new_txn.first,
        .profile = new_txn.second,
        .sent_at = system_clock::now(),
        .recv_at = system_clock::now(),
        .finished = false,
    };
    txns_.push_back(std::move(info));
  }

  if (!dry_run_) {
    socket_.set(zmq::sockopt::sndhwm, 0);
    socket_.set(zmq::sockopt::rcvhwm, 0);
    for (uint32_t p = 0; p < config_->num_partitions(); p++) {
      std::ostringstream endpoint_s;
      if (config_->protocol() == "ipc") {
        endpoint_s << "tcp://localhost:" << config_->server_port();
      } else {
        endpoint_s << "tcp://" << config_->address(region_, p) << ":" << config_->server_port();
      }
      auto endpoint = endpoint_s.str();
      LOG(INFO) << "Connecting to " << endpoint;
      socket_.connect(endpoint);
    }
    poller_.PushSocket(socket_);
  }

  poller_.AddTimeEvent(interval_, nullptr);
}

bool TxnGenerator::Loop() {
  auto res = poller_.Wait();

  if (res.num_zmq_events > 0) {
    if (api::Response res; RecvDeserializedProtoWithEmptyDelim(socket_, res)) {
      auto& info = txns_[res.stream_id()];
      if (info.finished) {
        LOG(ERROR) << "Received response for finished txn. Stream id: " << res.stream_id();
      } else {
        info.recv_at = system_clock::now();
        info.txn = res.mutable_txn()->release_txn();
        info.finished = true;
        ++num_recv_txns_;
      }
    }
  }

  if (!res.time_events.empty()) {
    if (cur_txn_ < txns_.size()) {
      auto& info = txns_[cur_txn_];

      // Send current txn
      if (!dry_run_) {
        api::Request req;
        req.mutable_txn()->set_allocated_txn(info.txn);
        req.set_stream_id(cur_txn_);
        SendSerializedProtoWithEmptyDelim(socket_, req);
        info.txn = nullptr;
      }
      info.sent_at = system_clock::now();

      // Schedule for next txn
      ++cur_txn_;
      poller_.AddTimeEvent(interval_, nullptr);
    }
  }

  if (num_recv_txns_ == txns_.size() || (dry_run_ && cur_txn_ == txns_.size())) {
    return true;
  }
  return false;
}

}  // namespace slog