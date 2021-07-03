#include "module/txn_generator.h"

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

bool RecordFinishedTxn(TxnGenerator::TxnInfo& info, int generator_id, Transaction* txn, bool is_dummy) {
  if (info.finished) {
    LOG(ERROR) << "Received response for finished txn";
    return false;
  }
  info.recv_at = system_clock::now();
  info.generator_id = generator_id;
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

static int generator_id = 0;

}  // namespace

TxnGenerator::TxnGenerator(std::unique_ptr<Workload>&& workload)
    : id_(generator_id++),
      workload_(std::move(workload)),
      num_sent_txns_(0),
      num_recv_txns_(0),
      elapsed_time_(std::chrono::nanoseconds(0)),
      timer_running_(false) {
  CHECK(workload_ != nullptr) << "Must provide a valid workload";
}
const Workload& TxnGenerator::workload() const { return *workload_; }
size_t TxnGenerator::num_sent_txns() const { return num_sent_txns_; }
size_t TxnGenerator::num_recv_txns() const { return num_recv_txns_; }
std::chrono::nanoseconds TxnGenerator::elapsed_time() const {
  if (timer_running_) {
    return std::chrono::steady_clock::now() - start_time_;
  }
  return elapsed_time_.load();
}

void TxnGenerator::StartTimer() {
  timer_running_ = true;
  start_time_ = std::chrono::steady_clock::now();
}

void TxnGenerator::StopTimer() {
  timer_running_ = false;
  elapsed_time_ = std::chrono::steady_clock::now() - start_time_;
}

bool TxnGenerator::timer_running() const { return timer_running_; }

SynchronousTxnGenerator::SynchronousTxnGenerator(const ConfigurationPtr& config, zmq::context_t& context,
                                                 std::unique_ptr<Workload>&& workload, uint32_t region,
                                                 uint32_t num_txns, int num_clients, int duration_s, bool dry_run)
    : TxnGenerator(std::move(workload)),
      config_(config),
      socket_(context, ZMQ_DEALER),
      poller_(kModuleTimeout),
      region_(region),
      num_txns_(num_txns),
      num_clients_(num_clients),
      duration_(duration_s * 1000),
      dry_run_(dry_run) {
  CHECK_GT(duration_s, 0) << "Duration must be set for synchronous txn generator";
}

SynchronousTxnGenerator::~SynchronousTxnGenerator() {
  for (auto& txn : generated_txns_) {
    delete txn.first;
    txn.first = nullptr;
  }
  for (auto& txn : txns_) {
    delete txn.txn;
    txn.txn = nullptr;
  }
}

void SynchronousTxnGenerator::SetUp() {
  if (num_txns_ <= 0) {
    LOG(INFO) << "No txn is pre-generated";
  } else {
    LOG(INFO) << "Generating " << num_txns_ << " transactions";
    for (size_t i = 0; i < num_txns_; i++) {
      generated_txns_.push_back(workload_->NextTransaction());
    }
  }

  if (!dry_run_) {
    ConnectToServer(config_, socket_, region_);
    poller_.PushSocket(socket_);
    for (int i = 0; i < num_clients_; i++) {
      SendNextTxn();
      std::this_thread::sleep_for(500us);
    }
  }

  LOG(INFO) << "Start sending transactions with " << num_clients_ << " concurrent clients";

  StartTimer();
}

bool SynchronousTxnGenerator::Loop() {
  if (dry_run_) {
    return true;
  }

  bool duration_reached = elapsed_time() >= duration_;
  if (poller_.NextEvent()) {
    if (api::Response res; RecvDeserializedProtoWithEmptyDelim(socket_, res)) {
      auto& info = txns_[res.stream_id()];
      if (RecordFinishedTxn(info, id_, res.mutable_txn()->release_txn(), config_->return_dummy_txn())) {
        num_recv_txns_++;
        if (!duration_reached) {
          SendNextTxn();
        }
      }
    }
  }

  if (duration_reached && num_recv_txns_ == txns_.size()) {
    StopTimer();
    return true;
  }
  return false;
}

void SynchronousTxnGenerator::SendNextTxn() {
  std::pair<Transaction*, TransactionProfile> selected_txn;

  api::Request req;
  req.set_stream_id(num_sent_txns());

  TxnInfo info;
  if (generated_txns_.empty()) {
    auto [txn, profile] = workload_->NextTransaction();
    req.mutable_txn()->set_allocated_txn(txn);
    info.profile = profile;
  } else {
    auto [txn, profile] = generated_txns_[num_sent_txns() % generated_txns_.size()];
    req.mutable_txn()->set_allocated_txn(new Transaction(*txn));
    info.profile = profile;
  }

  SendSerializedProtoWithEmptyDelim(socket_, req);

  info.sent_at = system_clock::now();
  info.txn = req.mutable_txn()->release_txn();

  txns_.push_back(std::move(info));

  ++num_sent_txns_;
}

ConstantRateTxnGenerator::ConstantRateTxnGenerator(const ConfigurationPtr& config, zmq::context_t& context,
                                                   unique_ptr<Workload>&& workload, uint32_t region, uint32_t num_txns,
                                                   int tps, int duration_s, bool dry_run)
    : TxnGenerator(std::move(workload)),
      config_(config),
      socket_(context, ZMQ_DEALER),
      poller_(kModuleTimeout),
      region_(region),
      num_txns_(num_txns),
      duration_(duration_s * 1000),
      dry_run_(dry_run) {
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
}

void ConstantRateTxnGenerator::SendNextTxn() {
  // If duration is set, keep sending txn until duration is reached, otherwise send until all generated txns are sent
  if (duration_ > 0ms) {
    if (elapsed_time() >= duration_) {
      return;
    }
  } else if (num_sent_txns() >= generated_txns_.size()) {
    return;
  }

  const auto& selected_txn = generated_txns_[num_sent_txns() % generated_txns_.size()];

  api::Request req;
  req.mutable_txn()->set_allocated_txn(new Transaction(*selected_txn.first));
  req.set_stream_id(num_sent_txns());
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

      if (!timer_running()) {
        StartTimer();
      }

      auto& info = txns_[res.stream_id()];
      num_recv_txns_ += RecordFinishedTxn(info, id_, res.mutable_txn()->release_txn(), config_->return_dummy_txn());
    }
  }

  bool stop = false;
  // If duration is set, keep sending txn until duration is reached, otherwise send until all generated txns are sent
  if (duration_ > 0ms) {
    bool duration_reached = elapsed_time() >= duration_;
    stop = duration_reached && (dry_run_ || num_recv_txns_ == num_sent_txns_);
  } else {
    stop = num_sent_txns_ >= generated_txns_.size() && (dry_run_ || num_recv_txns_ >= txns_.size());
  }
  if (stop) {
    StopTimer();
    return true;
  }
  return false;
}

}  // namespace slog