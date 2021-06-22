#pragma once

#include "common/configuration.h"
#include "common/metrics.h"
#include "connection/broker.h"
#include "data_structure/batch_log.h"
#include "module/base/networked_module.h"

namespace slog {

/**
 * A MultiHomeOrderer batches multi-home transactions and orders them globally
 * with paxos.
 *
 * INPUT:  ForwardTxn or ForwardBatch
 *
 * OUTPUT: For ForwardTxn, it has to contains a MULTI_HOME txn, which is put
 *         into a batch. The ID of this batch is sent to the global paxos
 *         process for ordering, and simultaneously, this batch is sent to
 *         the MultiHomeOrderer of all regions.
 *
 *         ForwardBatch'es are serialized into a log according to
 *         their globally orderred IDs and then forwarded to the Sequencer.
 */
class MultiHomeOrderer : public NetworkedModule {
 public:
  MultiHomeOrderer(const std::shared_ptr<Broker>& broker, const MetricsRepositoryManagerPtr& metrics_manager,
                   std::chrono::milliseconds poll_timeout = kModuleTimeout);

  std::string name() const override { return "MultiHomeOrderer"; }

 protected:
  void OnInternalRequestReceived(EnvelopePtr&& env) final;

 private:
  void ProcessForwardBatchData(EnvelopePtr&& env);
  void ProcessForwardBatchOrder(EnvelopePtr&& env);
  void ProcessStatsRequest(const internal::StatsRequest& stats_request);
  void AdvanceLog();

  void NewBatch();
  BatchId batch_id() const { return batch_id_counter_ * kMaxNumMachines + config()->local_machine_id(); }
  void AddToBatch(Transaction* txn);
  void SendBatch();

  std::vector<std::unique_ptr<internal::Batch>> batch_per_rep_;
  BatchId batch_id_counter_;
  int batch_size_;

  BatchLog multi_home_batch_log_;

  bool collecting_stats_;
  std::chrono::steady_clock::time_point batch_starting_time_;
  std::vector<int> stat_batch_sizes_;
  std::vector<float> stat_batch_durations_ms_;
};

}  // namespace slog