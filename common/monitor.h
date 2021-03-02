#include <glog/logging.h>

#include <chrono>

#include "common/configuration.h"
#include "proto/internal.pb.h"

namespace slog {

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

extern uint32_t gLocalMachineId;
extern uint64_t gDisabledTracingEvents;

void InitializeTracing(const ConfigurationPtr& config);

template <typename TxnOrBatch>
inline void TraceTxnEvent(TxnOrBatch txn, TransactionEvent event) {
  if ((gDisabledTracingEvents >> event) & 1) {
    return;
  }
  using Clock = std::chrono::system_clock;
  txn->mutable_events()->Add(event);
  txn->mutable_event_times()->Add(duration_cast<microseconds>(Clock::now().time_since_epoch()).count());
  txn->mutable_event_machines()->Add(gLocalMachineId);
}

#ifdef ENABLE_TRACING
#define INIT_TRACING(config) slog::InitializeTracing(config)
#define TRACE(txn, event) TraceTxnEvent(txn, event)
#else
#define INIT_TRACING(config)
#define TRACE(txn, event)
#endif

}  // namespace slog