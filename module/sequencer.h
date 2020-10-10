#pragma once

#include <list>

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
 *         For ForwardBatch, MULTI_HOME txns are extracted from the batch. 
 *         For each MULTI_HOME txn, a corresponding LockOnly txn is created
 *         and put into the same batch as the SINGLE_HOME txn above. The 
 *         MULTI_HOME txn is sent to all Interleavers in the SAME region.
 */
class Sequencer : public NetworkedModule {
public:
  Sequencer(
      const ConfigurationPtr& config,
      const std::shared_ptr<Broker>& broker);

protected:
  std::vector<zmq::socket_t> InitializeCustomSockets() final;

  void HandleInternalRequest(internal::Request&& req, MachineIdNum from) final;

  void HandleCustomSocket(zmq::socket_t& socket, size_t socket_index) final;

private:
  void NewBatch();
  BatchId NextBatchId();

  void ProcessMultiHomeBatch(internal::Request&& request);
  void PutSingleHomeTransactionIntoBatch(Transaction* txn);

  ConfigurationPtr config_;
  unique_ptr<internal::Batch> batch_;
  BatchId batch_id_counter_;

#ifdef ENABLE_REPLICATION_DELAY
  void DelaySingleHomeBatch(internal::Request&& request);
  void MaybeSendDelayedBatches();
  
  std::list<internal::Request> delayed_batches_;
#endif /* ENABLE_REPLICATION_DELAY */
};

} // namespace slog