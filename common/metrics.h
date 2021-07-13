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
  MetricsRepositoryManager(const std::string& config_name, const ConfigurationPtr& config);
  void RegisterCurrentThread();
  void AggregateAndFlushToDisk(const std::string& dir);

 private:
  const std::string config_name_;
  const ConfigurationPtr config_;
  sample_mask_t sample_mask_;
  std::unordered_map<std::thread::id, std::shared_ptr<MetricsRepository>> metrics_repos_;
  std::mutex mut_;
};

using MetricsRepositoryManagerPtr = std::shared_ptr<MetricsRepositoryManager>;

extern uint32_t gLocalMachineId;
extern uint64_t gEnabledEvents;

void InitializeRecording(const ConfigurationPtr& config);

template <typename TxnOrBatchInternalPtr>
inline void RecordTxnEvent(TxnOrBatchInternalPtr txn_internal, TransactionEvent event) {
  if (!((gEnabledEvents >> event) & 1)) {
    return;
  }
  if (txn_internal != nullptr) {
    auto now = std::chrono::system_clock::now().time_since_epoch().count();
    auto new_event = txn_internal->mutable_events()->Add();
    new_event->set_event(event);
    new_event->set_time(now);
    new_event->set_machine(gLocalMachineId);
    new_event->set_home(-1);
  }
  if (per_thread_metrics_repo != nullptr) {
    per_thread_metrics_repo->RecordTxnEvent(event);
  }
}

#ifdef ENABLE_TXN_EVENT_RECORDING
#define INIT_RECORDING(config) slog::InitializeRecording(config)
#define RECORD(txn, event) RecordTxnEvent(txn, event)
#else
#define INIT_RECORDING(config)
#define RECORD(txn, event)
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