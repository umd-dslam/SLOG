#include "common/configuration.h"
#include "proto/internal.pb.h"

namespace slog {

void MonitorThroughput();

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
#define INIT_TRACING(config) InitializeTracing(config)
#define TRACE(txn, event) TraceTxnEvent(txn, event)
#else
#define INIT_TRACING(config)
#define TRACE(txn, event)
#endif

}  // namespace slog