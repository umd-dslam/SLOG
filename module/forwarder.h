#pragma once

#include <random>
#include <unordered_map>

#include "common/configuration.h"
#include "common/types.h"
#include "connection/broker.h"
#include "module/base/networked_module.h"
#include "proto/transaction.pb.h"
#include "storage/lookup_master_index.h"

namespace slog {

/**
 * A Forwarder determines the type of a transaction (SINGLE_HOME vs. MULTI_HOME)
 * then forwards it to the appropriate module.
 *
 * To determine the type of a txn, it sends LookupMasterRequests to all Server
 * modules in the same region and aggregates the responses.
 *
 * INPUT:  ForwardTransaction and LookUpMasterRequest
 *
 * OUTPUT: If the txn is SINGLE_HOME, forward to the Sequencer in its home region.
 *         If the txn is MULTI_HOME, forward to the GlobalPaxos for ordering.
 *
 *         For LookUpMasterRequest, a LookUpMasterResponse is sent back to
 *         the requester.
 */
class Forwarder : public NetworkedModule {
 public:
  Forwarder(const ConfigurationPtr& config, const shared_ptr<Broker>& broker,
            const shared_ptr<LookupMasterIndex<Key, Metadata>>& lookup_master_index,
            int poll_timeout_ms = kModuleTimeoutMs);

 protected:
  void HandleInternalRequest(ReusableRequest&& req, MachineId from) final;
  void HandleInternalResponse(ReusableResponse&& res, MachineId from) final;

 private:
  void ProcessForwardTxn(ReusableRequest&& req);
  void ProcessLookUpMasterRequest(ReusableRequest&& req, MachineId from);

  /**
   * Pre-condition: transaction type is not UNKNOWN
   */
  void Forward(Transaction* txn);

  ConfigurationPtr config_;
  std::shared_ptr<LookupMasterIndex<Key, Metadata>> lookup_master_index_;

  std::unordered_map<TxnId, ReusableRequest> pending_transactions_;

  std::mt19937 rg_;
};

}  // namespace slog