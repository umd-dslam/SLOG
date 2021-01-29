#pragma once

#include <list>
#include <random>

#include "common/configuration.h"
#include "common/types.h"
#include "connection/broker.h"
#include "module/base/networked_module.h"

namespace slog {

/**
 * A Sequencer batches transactions before sending to the Interleaver.
 *
 * INPUT:  ForwardTxn or ForwardBatch
 *
 * OUTPUT: For ForwardTxn, it has to contains a SINGLE_HOME txn, which is put
 *         into a batch. The ID of this batch is sent to the local paxos
 *         process for ordering, and simultaneously, this batch is sent to the
 *         Interleaver of all machines across all regions.
 *
 *         For each MULTI_HOME txn, a corresponding LockOnly txn is created
 *         and put into the same batch as the SINGLE_HOME txn above. The
 *         MULTI_HOME txn is sent to all Interleavers in the SAME region.
 */
class Sequencer : public NetworkedModule {
 public:
  Sequencer(const ConfigurationPtr& config, const std::shared_ptr<Broker>& broker, milliseconds batch_timeout,
            milliseconds poll_timeout = kModuleTimeout);

 protected:
  void HandleInternalRequest(EnvelopePtr&& env) final;

 private:
  void NewBatch();
  BatchId batch_id() const { return batch_id_counter_ * kMaxNumMachines + config_->local_machine_id(); }
  void SendBatch();
  EnvelopePtr NewBatchRequest(internal::Batch* batch);
  bool SendBatchDelayed();

  ConfigurationPtr config_;
  milliseconds batch_timeout_;
  std::vector<unique_ptr<internal::Batch>> partitioned_batch_;
  BatchId batch_id_counter_;
  int batch_size_;
  bool batch_scheduled_;

  std::mt19937 rg_;
};

}  // namespace slog