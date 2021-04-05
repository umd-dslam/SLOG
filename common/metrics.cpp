#include "metrics.h"

#include <algorithm>
#include <list>
#include <random>

#include "common/csv_writer.h"
#include "common/proto_utils.h"
#include "glog/logging.h"
#include "proto/internal.pb.h"

namespace slog {

using time_point_t = std::chrono::system_clock::time_point;

class TransactionEventMetrics {
 public:
  TransactionEventMetrics(const sample_mask_t& sample_mask, uint32_t local_replica, uint32_t local_partition)
      : sample_mask_(sample_mask),
        local_replica_(local_replica),
        local_partition_(local_partition),
        sample_count_(TransactionEvent_descriptor()->value_count(), 0) {}

  time_point_t RecordEvent(TransactionEvent event) {
    auto now = std::chrono::system_clock::now();
    auto sample_index = static_cast<size_t>(event);
    DCHECK_LT(sample_count_[sample_index], sample_mask_.size());
    if (sample_mask_[sample_count_[sample_index]++]) {
      txn_events_.push_back({.event = event,
                             .time = now.time_since_epoch().count(),
                             .partition = local_partition_,
                             .replica = local_replica_});
    }
    return now;
  }

  struct Data {
    TransactionEvent event;
    int64_t time;
    uint32_t partition;
    uint32_t replica;
  };

  std::list<Data>& data() { return txn_events_; }

 private:
  sample_mask_t sample_mask_;
  uint32_t local_replica_;
  uint32_t local_partition_;
  std::vector<uint8_t> sample_count_;
  std::list<Data> txn_events_;
};

/**
 *  MetricsRepository
 */

MetricsRepository::MetricsRepository(const ConfigurationPtr& config, const sample_mask_t& sample_mask)
    : config_(config),
      sample_mask_(sample_mask),
      txn_event_metrics_(new TransactionEventMetrics(sample_mask, config->local_replica(), config->local_partition())) {
}

time_point_t MetricsRepository::RecordTxnEvent(TransactionEvent event) {
  std::lock_guard<SpinLatch> guard(latch_);
  return txn_event_metrics_->RecordEvent(event);
}

std::unique_ptr<TransactionEventMetrics> MetricsRepository::Reset() {
  auto new_txn_event_metrics =
      std::make_unique<TransactionEventMetrics>(sample_mask_, config_->local_replica(), config_->local_partition());
  std::lock_guard<SpinLatch> guard(latch_);
  txn_event_metrics_.swap(new_txn_event_metrics);
  return new_txn_event_metrics;
}

thread_local std::shared_ptr<MetricsRepository> per_thread_metrics_repo;

/**
 *  MetricsRepositoryManager
 */

MetricsRepositoryManager::MetricsRepositoryManager(const ConfigurationPtr& config) : config_(config) {
  sample_mask_.fill(false);
  for (uint32_t i = 0; i < config_->sample_rate() * kSampleMaskSize / 100; i++) {
    sample_mask_[i] = true;
  }
  auto rd = std::random_device{};
  auto rng = std::default_random_engine{rd()};
  std::shuffle(sample_mask_.begin(), sample_mask_.end(), rng);
}

void MetricsRepositoryManager::RegisterCurrentThread() {
  std::lock_guard<std::mutex> guard(mut_);
  const auto thread_id = std::this_thread::get_id();
  auto ins = metrics_repos_.try_emplace(thread_id, config_, new MetricsRepository(config_, sample_mask_));
  per_thread_metrics_repo = ins.first->second;
}

void MetricsRepositoryManager::AggregateAndFlushToDisk(const std::string& dir) {
  try {
    CSVWriter txn_events_csv(dir + "/txn_events.csv", {"event_id", "time", "partition", "replica"});
    CSVWriter event_names_csv(dir + "/event_names.csv", {"id", "event"});

    std::list<TransactionEventMetrics::Data> txn_events_data;
    std::lock_guard<std::mutex> guard(mut_);
    for (auto& kv : metrics_repos_) {
      auto metrics = kv.second->Reset();
      txn_events_data.splice(txn_events_data.end(), metrics->data());
    }

    std::unordered_map<int, string> event_names;
    for (const auto& data : txn_events_data) {
      txn_events_csv << static_cast<int>(data.event) << data.time << data.partition << data.replica << csvendl;
      event_names[data.event] = ENUM_NAME(data.event, TransactionEvent);
    }
    for (auto e : event_names) {
      event_names_csv << e.first << e.second << csvendl;
    }
    LOG(INFO) << "Metrics written to: \"" << dir << "/\"";
  } catch (std::runtime_error& e) {
    LOG(ERROR) << e.what();
  }
}

/**
 * Initialization
 */

uint32_t gLocalMachineId = 0;
uint64_t gDisabledEvents = 0;

void InitializeRecording(const ConfigurationPtr& config) {
  gLocalMachineId = config->local_machine_id();
  auto events = config->disabled_events();
  for (auto e : events) {
    if (e == TransactionEvent::ALL) {
      gDisabledEvents = ~0;
      return;
    }
    gDisabledEvents |= (1 << e);
  }
}

}  // namespace slog