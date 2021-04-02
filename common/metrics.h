#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/configuration.h"
#include "common/spin_latch.h"
#include "proto/transaction.pb.h"

namespace slog {

constexpr uint32_t kSampleMaskSize = 1 << 8;
using sample_mask_t = std::array<bool, kSampleMaskSize>;

class TransactionEventMetrics;

/**
 * Repository of metrics per thread
 */
class MetricsRepository {
 public:
  MetricsRepository(const ConfigurationPtr& config, const sample_mask_t& sample_mask);

  std::chrono::system_clock::time_point RecordTxnEvent(TransactionEvent event);
  std::unique_ptr<TransactionEventMetrics> Reset();

 private:
  const ConfigurationPtr config_;
  sample_mask_t sample_mask_;
  SpinLatch latch_;

  std::unique_ptr<TransactionEventMetrics> txn_event_metrics_;
};

extern thread_local std::shared_ptr<MetricsRepository> per_thread_metrics_repo;

/**
 * Handles thread registering, aggregates results, and output results to files
 */
class MetricsRepositoryManager {
 public:
  MetricsRepositoryManager(const ConfigurationPtr& config);
  void RegisterCurrentThread();
  void AggregateAndFlushToDisk(const std::string& dir);

 private:
  const ConfigurationPtr config_;
  sample_mask_t sample_mask_;
  std::unordered_map<std::thread::id, std::shared_ptr<MetricsRepository>> metrics_repos_;
  std::mutex mut_;
};

using MetricsRepositoryManagerPtr = std::shared_ptr<MetricsRepositoryManager>;

extern uint32_t gLocalMachineId;
extern uint64_t gDisabledTracingEvents;

void InitializeTracing(const ConfigurationPtr& config);

template <typename TxnOrBatchPtr>
inline void TraceTxnEvent(TxnOrBatchPtr txn, TransactionEvent event) {
  using Clock = std::chrono::system_clock;
  auto now = duration_cast<microseconds>(Clock::now().time_since_epoch()).count();
  if (!((gDisabledTracingEvents >> event) & 1) && txn != nullptr) {
    txn->mutable_events()->Add(event);
    txn->mutable_event_times()->Add(now);
    txn->mutable_event_machines()->Add(gLocalMachineId);
  }
  if (per_thread_metrics_repo != nullptr) {
    per_thread_metrics_repo->RecordTxnEvent(event);
  }
}

#ifdef ENABLE_TRACING
#define INIT_TRACING(config) slog::InitializeTracing(config)
#define TRACE(txn, event) TraceTxnEvent(txn, event)
#else
#define INIT_TRACING(config)
#define TRACE(txn, event)
#endif

// Helper function for quickly monitor throughput at a certain place
// TODO: use thread_local instead of static
#define MONITOR_THROUGHPUT()                                                                                \
  static int TP_COUNTER = 0;                                                                                \
  static int TP_LAST_COUNTER = 0;                                                                           \
  static std::chrono::steady_clock::time_point TP_LAST_LOG_TIME;                                            \
  TP_COUNTER++;                                                                                             \
  auto TP_LOG_SPAN = std::chrono::steady_clock::now() - TP_LAST_LOG_TIME;                                   \
  if (TP_LOG_SPAN > 1s) {                                                                                   \
    LOG(INFO) << "Throughput: "                                                                             \
              << (TP_COUNTER - TP_LAST_COUNTER) / std::chrono::duration_cast<seconds>(TP_LOG_SPAN).count(); \
    TP_LAST_COUNTER = TP_COUNTER;                                                                           \
    TP_LAST_LOG_TIME = std::chrono::steady_clock::now();                                                    \
  }

}  // namespace slog