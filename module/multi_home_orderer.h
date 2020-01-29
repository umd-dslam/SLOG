#pragma once

#include "common/batch_log.h"
#include "common/configuration.h"
#include "connection/broker.h"
#include "module/base/basic_module.h"
#include "paxos/paxos_client.h"

namespace slog {

class MultiHomeOrderer : public BasicModule {
public:
  MultiHomeOrderer(
      shared_ptr<Configuration> config,
      Broker& broker);

protected:
  void HandleInternalRequest(
      internal::Request&& req,
      string&& from_machine_id) final;

  void HandlePeriodicWakeUp() final;

private:
  void ProcessForwardBatchRequest(
      internal::ForwardBatchRequest* forward_batch);

  BatchId NextBatchId();

  shared_ptr<Configuration> config_;
  unique_ptr<PaxosClient> global_paxos_;
  unique_ptr<internal::Batch> batch_;
  BatchId batch_id_counter_;

  BatchLog multi_home_batch_log_;
};

} // namespace slog