#pragma once

#include <unordered_set>

#include "common/configuration.h"
#include "common/types.h"
#include "connection/broker.h"
#include "module/base/basic_module.h"
#include "paxos/paxos_client.h"

namespace slog {

class Sequencer : public BasicModule {
public:
  Sequencer(
      shared_ptr<Configuration> config,
      Broker& broker);

protected:
  void HandleInternalRequest(
      internal::Request&& req,
      string&& from_machine_id) final;

  void HandleInternalResponse(
      internal::Response&& res,
      string&& from_machine_id) final;

  void HandlePeriodicWakeUp() final;

private:
  void PutTransactionIntoBatch(Transaction* txn);

  BatchId NextBatchId();

  shared_ptr<Configuration> config_;
  unique_ptr<PaxosClient> local_paxos_;
  unique_ptr<internal::Batch> batch_;
  
  BatchId batch_id_counter_;
  BatchId current_batch_id_;
  std::unordered_set<uint32_t> pending_acks_;
};

} // namespace slog