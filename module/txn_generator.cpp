#include "module/txn_generator.h"

#include <glog/logging.h>

#include <sstream>

#include "common/constants.h"
#include "connection/zmq_utils.h"
#include "proto/api.pb.h"

using std::shared_ptr;
using std::unique_ptr;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::steady_clock;
using std::chrono::system_clock;
using std::chrono::operator""ms;
using std::chrono::operator""us;

namespace slog {
namespace {
void ConnectToServer(const ConfigurationPtr& config, zmq::socket_t& socket, uint32_t region) {
  socket.set(zmq::sockopt::sndhwm, 0);
  socket.set(zmq::sockopt::rcvhwm, 0);
  for (uint32_t p = 0; p < config->num_partitions(); p++) {
    std::ostringstream endpoint_s;
    if (config->protocol() == "ipc") {
      endpoint_s << "tcp://localhost:" << config->server_port();
    } else {
      endpoint_s << "tcp://" << config->address(region, p) << ":" << config->server_port();
    }
    auto endpoint = endpoint_s.str();
    LOG(INFO) << "Connecting to " << endpoint;
    socket.connect(endpoint);
  }
}

bool RecordFinishedTxn(TxnGenerator::TxnInfo& info, Transaction* txn, bool is_dummy) {
  if (info.finished) {
    LOG(ERROR) << "Received response for finished txn";
    return false;
  }
  info.recv_at = system_clock::now();
  if (is_dummy) {
    if (info.txn == nullptr) {
      LOG(ERROR) << "No transaction in the txn info";
    } else {
      info.txn->set_status(txn->status());
      info.txn->mutable_internal()->CopyFrom(txn->internal());
      delete txn;
    }
  } else {
    delete info.txn;
    info.txn = txn;
  }
  info.finished = true;
  return true;
}

}  // namespace

SynchronizedTxnGenerator::SynchronizedTxnGenerator(const ConfigurationPtr& config, zmq::context_t& context,
                                                   std::unique_ptr<Workload>&& workload, uint32_t region,
                                                   uint32_t num_txns, int num_clients, int duration_s, bool dry_run)
    : Module("Synchronized-Txn-Generator"),
      config_(config),
      socket_(context, ZMQ_DEALER),
      workload_(std::move(workload)),
      poller_(kModuleTimeout),
      region_(region),
      num_txns_(num_txns),
      num_clients_(num_clients),
      duration_(duration_s * 1000),
      dry_run_(dry_run),
      num_sent_txns_(0),
      num_recv_txns_(0) {
  CHECK(workload_ != nullptr) << "Must provide a valid workload";
  CHECK_GT(duration_s, 0) << "Duration must be set for synchronized txn generator";
}

SynchronizedTxnGenerator::~SynchronizedTxnGenerator() {
  for (auto& txn : generated_txns_) {
    delete txn.first;
    txn.first = nullptr;
  }
  for (auto& txn : txns_) {
    delete txn.txn;
    txn.txn = nullptr;
  }
}

void SynchronizedTxnGenerator::SetUp() {
  CHECK_GT(num_txns_, 0) << "There must be at least one transaction";
  LOG(INFO) << "Generating " << num_txns_ << " transactions";
  for (size_t i = 0; i < num_txns_; i++) {
    generated_txns_.push_back(workload_->NextTransaction());
  }

  if (!dry_run_) {
    ConnectToServer(config_, socket_, region_);
    poller_.PushSocket(socket_);
    for (int i = 0; i < num_clients_; i++) {
      SendNextTxn();
      std::this_thread::sleep_for(100us);
    }
  }

  LOG(INFO) << "Start sending transactions with " << num_clients_ << " concurrent clients";
  start_time_ = steady_clock::now();
}

bool SynchronizedTxnGenerator::Loop() {
  if (dry_run_) {
    return true;
  }

  bool duration_reached = steady_clock::now() - start_time_ >= duration_;
  if (poller_.NextEvent()) {
    if (api::Response res; RecvDeserializedProtoWithEmptyDelim(socket_, res)) {
      auto& info = txns_[res.stream_id()];
      if (RecordFinishedTxn(info, res.mutable_txn()->release_txn(), config_->return_dummy_txn())) {
        num_recv_txns_++;
        if (!duration_reached) {
          SendNextTxn();
        }
      }
    }
  }

  if (duration_reached && num_recv_txns_ == txns_.size()) {
    elapsed_time_ = duration_cast<milliseconds>(steady_clock::now() - start_time_);
    return true;
  }
  return false;
}

void SynchronizedTxnGenerator::SendNextTxn() {
  const auto& selected_txn = generated_txns_[num_sent_txns_ % generated_txns_.size()];

  api::Request req;
  req.mutable_txn()->set_allocated_txn(new Transaction(*selected_txn.first));
  req.set_stream_id(num_sent_txns_);
  SendSerializedProtoWithEmptyDelim(socket_, req);

  TxnInfo info;
  info.txn = req.mutable_txn()->release_txn();
  info.profile = selected_txn.second;
  info.sent_at = system_clock::now();
  txns_.push_back(std::move(info));

  ++num_sent_txns_;
}

ConstantRateTxnGenerator::ConstantRateTxnGenerator(const ConfigurationPtr& config, zmq::context_t& context,
                                                   unique_ptr<Workload>&& workload, uint32_t region, uint32_t num_txns,
                                                   int tps, int duration_s, bool dry_run)
    : Module("Txn-Generator"),
      config_(config),
      socket_(context, ZMQ_DEALER),
      workload_(std::move(workload)),
      poller_(kModuleTimeout),
      region_(region),
      num_txns_(num_txns),
      duration_(duration_s * 1000),
      dry_run_(dry_run),
      num_sent_txns_(0),
      num_recv_txns_(0) {
  CHECK(workload_ != nullptr) << "Must provide a valid workload";

  CHECK_LT(tps, 1000000) << "Transaction/sec is too high (max. 1000000)";
  int overhead_estimate = !dry_run_ * 10;
  if (1000000 / tps > overhead_estimate) {
    interval_ = std::chrono::microseconds(1000000 / tps - overhead_estimate);
  } else {
    interval_ = 0us;
  }
}

ConstantRateTxnGenerator::~ConstantRateTxnGenerator() {
  for (auto& txn : generated_txns_) {
    delete txn.first;
    txn.first = nullptr;
  }
  for (auto& txn : txns_) {
    delete txn.txn;
    txn.txn = nullptr;
  }
}

void ConstantRateTxnGenerator::SetUp() {
  CHECK_GT(num_txns_, 0) << "There must be at least one transaction";
  LOG(INFO) << "Generating " << num_txns_ << " transactions";
  for (size_t i = 0; i < num_txns_; i++) {
    generated_txns_.push_back(workload_->NextTransaction());
  }

  if (!dry_run_) {
    ConnectToServer(config_, socket_, region_);
    poller_.PushSocket(socket_);
  }

  // Schedule sending new txns
  poller_.AddTimedCallback(interval_, [this]() { SendNextTxn(); });
  LOG(INFO) << "Start sending transactions";
  start_time_ = steady_clock::now();
}

void ConstantRateTxnGenerator::SendNextTxn() {
  // If duration is set, keep sending txn until duration is reached, otherwise send until all generated txns are sent
  if (duration_ > 0ms) {
    if (steady_clock::now() - start_time_ >= duration_) {
      return;
    }
  } else if (num_sent_txns_ >= generated_txns_.size()) {
    return;
  }

  const auto& selected_txn = generated_txns_[num_sent_txns_ % generated_txns_.size()];

  api::Request req;
  req.mutable_txn()->set_allocated_txn(new Transaction(*selected_txn.first));
  req.set_stream_id(num_sent_txns_);
  if (!dry_run_) {
    SendSerializedProtoWithEmptyDelim(socket_, req);
  }

  TxnInfo info;
  info.txn = req.mutable_txn()->release_txn();
  info.profile = selected_txn.second;
  info.sent_at = system_clock::now();
  txns_.push_back(std::move(info));

  ++num_sent_txns_;

  poller_.AddTimedCallback(interval_, [this]() { SendNextTxn(); });
}

bool ConstantRateTxnGenerator::Loop() {
  if (poller_.NextEvent()) {
    if (api::Response res; RecvDeserializedProtoWithEmptyDelim(socket_, res)) {
      CHECK_LT(res.stream_id(), txns_.size());
      auto& info = txns_[res.stream_id()];
      num_recv_txns_ += RecordFinishedTxn(info, res.mutable_txn()->release_txn(), config_->return_dummy_txn());
    }
  }

  bool stop = false;
  // If duration is set, keep sending txn until duration is reached, otherwise send until all generated txns are sent
  if (duration_ > 0ms) {
    bool duration_reached = steady_clock::now() - start_time_ >= duration_;
    stop = duration_reached && (dry_run_ || num_recv_txns_ == num_sent_txns_);
  } else {
    stop = num_sent_txns_ >= generated_txns_.size() && (dry_run_ || num_recv_txns_ >= txns_.size());
  }
  if (stop) {
    elapsed_time_ = duration_cast<milliseconds>(steady_clock::now() - start_time_);
    return true;
  }
  return false;
}

}  // namespace slog