#pragma once

#include "common/configuration.h"
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
  MultiHomeOrderer(const ConfigurationPtr& config, const std::shared_ptr<Broker>& broker, milliseconds batch_timeout,
                   milliseconds poll_timeout = kModuleTimeout);

 protected:
  void HandleInternalRequest(EnvelopePtr&& env) final;

 private:
  void NewBatch();
  BatchId batch_id() const { return batch_id_counter_ * kMaxNumMachines + config_->local_machine_id(); }
  void AddToBatch(Transaction* txn);
  void SendBatch();

  void ProcessForwardBatch(EnvelopePtr&& env);

  ConfigurationPtr config_;
  milliseconds batch_timeout_;
  std::vector<unique_ptr<internal::Batch>> batch_per_rep_;
  BatchId batch_id_counter_;
  int batch_size_;
  bool batch_scheduled_;

  BatchLog multi_home_batch_log_;
};

}  // namespace slog