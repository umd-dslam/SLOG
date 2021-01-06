#pragma once

#include <atomic>
#include <vector>
#include <zmq.hpp>

#include "connection/poller.h"
#include "module/base/module.h"
#include "workload/workload.h"

namespace slog {

class TxnGenerator : public Module {
 public:
  TxnGenerator(const ConfigurationPtr& config, zmq::context_t& context, std::unique_ptr<Workload>&& workload,
               uint32_t region, uint32_t num_txns, uint32_t tps, bool dry_run);
  void SetUp() final;
  bool Loop() final;

  size_t num_sent_txns() const { return cur_txn_; }
  size_t num_recv_txns() const { return num_recv_txns_; }

  using TimePoint = std::chrono::system_clock::time_point;
  struct TxnInfo {
    Transaction* txn;
    TransactionProfile profile;
    TimePoint sent_at;
    TimePoint recv_at;
    bool finished;
  };

  const std::vector<TxnInfo>& txns() const { return txns_; }

 private:
  ConfigurationPtr config_;
  zmq::socket_t socket_;
  std::unique_ptr<Workload> workload_;
  Poller poller_;
  std::chrono::microseconds interval_;
  uint32_t region_;
  uint32_t num_txns_;
  bool dry_run_;

  std::vector<TxnInfo> txns_;
  std::atomic<size_t> cur_txn_;
  std::atomic<size_t> num_recv_txns_;
};

}  // namespace slog