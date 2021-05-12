#pragma once

#include <atomic>
#include <vector>
#include <zmq.hpp>

#include "connection/poller.h"
#include "module/base/module.h"
#include "workload/workload.h"

namespace slog {

class TxnGenerator {
 public:
  struct TxnInfo {
    Transaction* txn = nullptr;
    TransactionProfile profile;
    std::chrono::system_clock::time_point sent_at;
    std::chrono::system_clock::time_point recv_at;
    bool finished = false;
  };

  virtual const std::vector<TxnInfo>& txns() const = 0;
  virtual size_t num_sent_txns() const = 0;
  virtual size_t num_recv_txns() const = 0;
  virtual std::chrono::milliseconds elapsed_time() const = 0;
};

// This generators simulates synchronized clients, each of which sends a new
// txn only after it receives response from the previous txn
class SynchronizedTxnGenerator : public Module, public TxnGenerator {
 public:
  SynchronizedTxnGenerator(const ConfigurationPtr& config, zmq::context_t& context,
                           std::unique_ptr<Workload>&& workload, uint32_t region, uint32_t num_txns, int num_clients,
                           int duration_s, bool dry_run);
  ~SynchronizedTxnGenerator();
  void SetUp() final;
  bool Loop() final;

  size_t num_sent_txns() const final { return num_sent_txns_; }
  size_t num_recv_txns() const final { return num_recv_txns_; }
  std::chrono::milliseconds elapsed_time() const final {
    if (elapsed_time_.load() == std::chrono::milliseconds(0)) {
      return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time_);
    }
    return elapsed_time_;
  }
  const std::vector<TxnInfo>& txns() const final { return txns_; }

 private:
  void SendNextTxn();

  ConfigurationPtr config_;
  zmq::socket_t socket_;
  std::unique_ptr<Workload> workload_;
  Poller poller_;
  uint32_t region_;
  uint32_t num_txns_;
  int num_clients_;
  std::chrono::milliseconds duration_;
  bool dry_run_;
  std::chrono::steady_clock::time_point start_time_;

  std::vector<std::pair<Transaction*, TransactionProfile>> generated_txns_;
  std::vector<TxnInfo> txns_;
  std::atomic<size_t> num_sent_txns_;
  std::atomic<size_t> num_recv_txns_;
  std::atomic<std::chrono::milliseconds> elapsed_time_;
};

// This generator sends txn at a constant rate
class ConstantRateTxnGenerator : public Module, public TxnGenerator {
 public:
  ConstantRateTxnGenerator(const ConfigurationPtr& config, zmq::context_t& context,
                           std::unique_ptr<Workload>&& workload, uint32_t region, uint32_t num_txns, int tps,
                           int duration_s, bool dry_run);
  ~ConstantRateTxnGenerator();

  void SetUp() final;
  bool Loop() final;

  size_t num_sent_txns() const final { return num_sent_txns_; }
  size_t num_recv_txns() const final { return num_recv_txns_; }
  std::chrono::milliseconds elapsed_time() const final {
    if (elapsed_time_.load() == std::chrono::milliseconds(0)) {
      return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time_);
    }
    return elapsed_time_;
  }

  const std::vector<TxnInfo>& txns() const final { return txns_; }

 private:
  void SendNextTxn();

  ConfigurationPtr config_;
  zmq::socket_t socket_;
  std::unique_ptr<Workload> workload_;
  Poller poller_;
  std::chrono::microseconds interval_;
  uint32_t region_;
  uint32_t num_txns_;
  std::chrono::milliseconds duration_;
  bool dry_run_;
  std::chrono::steady_clock::time_point start_time_;

  std::vector<std::pair<Transaction*, TransactionProfile>> generated_txns_;
  std::vector<TxnInfo> txns_;
  std::atomic<size_t> num_sent_txns_;
  std::atomic<size_t> num_recv_txns_;
  std::atomic<std::chrono::milliseconds> elapsed_time_;
};

}  // namespace slog