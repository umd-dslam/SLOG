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
 * A Forwarder determines the type of a transaction (single-home vs. multi-home)
 * then forwards it to the appropriate module.
 *
 * To determine the type of a txn, it sends LookupMasterRequests to other Forwarder
 * modules in the same region and aggregates the responses.
 *
 * INPUT:  ForwardTransaction and LookUpMasterRequest
 *
 * OUTPUT: If the txn is single-home, forward to the Sequencer in its home region.
 *         If the txn is multi-home, forward to the MultiHomeOrderer for ordering;
 *         if bypass_mh_orderer is set to true in the config, the multi-home txn is
 *         sent directly to the involved regions.
 *
 *         For LookUpMasterRequest, a LookUpMasterResponse is sent back to
 *         the requester.
 */
class Forwarder : public NetworkedModule {
 public:
  Forwarder(const ConfigurationPtr& config, const shared_ptr<Broker>& broker,
            const shared_ptr<LookupMasterIndex<Key, Metadata>>& lookup_master_index, milliseconds batch_timeout,
            milliseconds poll_timeout_ms = kModuleTimeout);

 protected:
  void HandleInternalRequest(EnvelopePtr&& env) final;
  void HandleInternalResponse(EnvelopePtr&& env) final;

 private:
  void ProcessForwardTxn(EnvelopePtr&& env);
  void ProcessLookUpMasterRequest(EnvelopePtr&& env);

  /**
   * Pre-condition: transaction type is not UNKNOWN
   */
  void Forward(EnvelopePtr&& env);

  ConfigurationPtr config_;
  std::shared_ptr<LookupMasterIndex<Key, Metadata>> lookup_master_index_;
  milliseconds batch_timeout_;

  std::unordered_map<TxnId, EnvelopePtr> pending_transactions_;
  std::vector<internal::Envelope> partitioned_lookup_request_;
  bool lookup_request_scheduled_;

  std::mt19937 rg_;
};

}  // namespace slog