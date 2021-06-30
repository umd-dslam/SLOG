#pragma once

#include <glog/logging.h>

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
    int generator_id;
  };

  TxnGenerator(std::unique_ptr<Workload>&& workload);
  const Workload& workload() const;
  size_t num_sent_txns() const;
  size_t num_recv_txns() const;
  std::chrono::nanoseconds elapsed_time() const;

  virtual const std::vector<TxnInfo>& txn_infos() const = 0;

 protected:
  void StartTimer();
  void StopTimer();
  bool timer_running() const;

  int id_;
  std::unique_ptr<Workload> workload_;
  std::atomic<size_t> num_sent_txns_;
  std::atomic<size_t> num_recv_txns_;

 private:
  std::chrono::steady_clock::time_point start_time_;
  std::atomic<std::chrono::nanoseconds> elapsed_time_;
  bool timer_running_;
};

// This generators simulates synchronous clients, each of which sends a new
// txn only after it receives response from the previous txn
class SynchronousTxnGenerator : public Module, public TxnGenerator {
 public:
  /**
   * If num_txns is set to 0, the txns are generated on-the-fly
   */
  SynchronousTxnGenerator(const ConfigurationPtr& config, zmq::context_t& context, std::unique_ptr<Workload>&& workload,
                          uint32_t region, uint32_t num_txns, int num_clients, int duration_s, bool dry_run);
  ~SynchronousTxnGenerator();
  void SetUp() final;
  bool Loop() final;
  const std::vector<TxnInfo>& txn_infos() const final { return txns_; }
  std::string name() const final { return "Synchronous-Txn-Generator"; }

 private:
  void SendNextTxn();

  ConfigurationPtr config_;
  zmq::socket_t socket_;
  Poller poller_;
  uint32_t region_;
  uint32_t num_txns_;
  int num_clients_;
  std::chrono::milliseconds duration_;
  bool dry_run_;
  std::vector<std::pair<Transaction*, TransactionProfile>> generated_txns_;
  std::vector<TxnInfo> txns_;
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
  const std::vector<TxnInfo>& txn_infos() const final { return txns_; }
  std::string name() const final { return "Constant-Txn-Generator"; }

 private:
  void SendNextTxn();

  ConfigurationPtr config_;
  zmq::socket_t socket_;
  Poller poller_;
  std::chrono::microseconds interval_;
  uint32_t region_;
  uint32_t num_txns_;
  std::chrono::milliseconds duration_;
  bool dry_run_;

  std::vector<std::pair<Transaction*, TransactionProfile>> generated_txns_;
  std::vector<TxnInfo> txns_;
};

}  // namespace slog