#pragma once

#include <unordered_set>

#include "common/configuration.h"
#include "common/types.h"
#include "connection/broker.h"
#include "module/base/basic_module.h"
#include "paxos/paxos_client.h"

namespace slog {

/**
 * A Sequencer batches transactions before sending to the Scheduler.
 * 
 * INPUT: ForwardTxn or ForwardBatch
 * 
 * OUTPUT: For ForwardTxn, a SINGLE_HOME txn is expected and then put 
 *         into a batch before sending to the Scheduler in the same
 *         machine.
 * 
 *         For ForwardBatch, MULTI_HOME txns are expected. For each txn,
 *         a corresponding LockOnly txn is created and put into the
 *         same batch as the SINGLE_HOME txn above. The MULTI_HOME txn
 *         is sent to ALL Schedulers in the SAME region.
 */
class Sequencer : public BasicModule {
public:
  Sequencer(ConfigurationPtr config, Broker& broker);

protected:
  void HandleInternalRequest(
      internal::Request&& req,
      string&& from_machine_id) final;

  void HandlePeriodicWakeUp() final;

private:
  void NewBatch();
  BatchId NextBatchId();

  void ProcessMultiHomeBatch(internal::Request&& request);
  void PutSingleHomeTransactionIntoBatch(Transaction* txn);

  ConfigurationPtr config_;
  unique_ptr<PaxosClient> local_paxos_;
  unique_ptr<internal::Batch> batch_;
  BatchId batch_id_counter_;
};

} // namespace slog