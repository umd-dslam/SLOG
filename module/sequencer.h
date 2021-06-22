#pragma once

#include <list>
#include <random>

#include "common/configuration.h"
#include "common/metrics.h"
#include "common/sharder.h"
#include "common/types.h"
#include "connection/broker.h"
#include "module/base/networked_module.h"

namespace slog {

/**
 * A Sequencer batches transactions before sending to the Interleaver.
 *
 * INPUT:  ForwardTxn
 *
 * OUTPUT: For a single-home txn, it is put into a batch. The ID of this batch is
 *         sent to the local paxos process for ordering. Simultaneously, this batch
 *         is sent to the Interleaver of all machines across all regions.
 *
 *         For a multi-home txn, a corresponding lock-only txn is created and then goes
 *         through the same process as a single-home txn above.
 */
class Sequencer : public NetworkedModule {
 public:
  Sequencer(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
            const MetricsRepositoryManagerPtr& metrics_manager,
            std::chrono::milliseconds poll_timeout = kModuleTimeout);

  std::string name() const override { return "Sequencer"; }

 protected:
  void OnInternalRequestReceived(EnvelopePtr&& env) final;

 private:
  void BatchTxn(Transaction* txn);
  void ProcessStatsRequest(const internal::StatsRequest& stats_request);

  void NewBatch();
  BatchId batch_id() const { return batch_id_counter_ * kMaxNumMachines + config()->local_machine_id(); }
  void SendBatch();
  EnvelopePtr NewBatchForwardingMessage(std::vector<internal::Batch*>&& batch);

  const SharderPtr sharder_;
  std::vector<std::unique_ptr<internal::Batch>> partitioned_batch_;
  BatchId batch_id_counter_;
  int batch_size_;

  std::mt19937 rg_;

  bool collecting_stats_;
  std::chrono::steady_clock::time_point batch_starting_time_;
  std::vector<int> stat_batch_sizes_;
  std::vector<float> stat_batch_durations_ms_;
};

}  // namespace slog