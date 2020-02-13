#pragma once

#include <random>
#include <unordered_map>

#include "common/configuration.h"
#include "common/types.h"
#include "connection/broker.h"
#include "module/base/basic_module.h"
#include "proto/transaction.pb.h"

using std::shared_ptr;
using std::unordered_map;

namespace slog {

/**
 * A Forwarder determines the type of a transaction (SINGLE_HOME vs. MULTI_HOME)
 * then forwards it to the appropriate module.
 * 
 * To determine the type of a txn, it sends LookupMasterRequests to all Server
 * modules in the same region and aggregates the responses.
 * 
 * INPUT:  ForwardTransaction
 * 
 * OUTPUT: If the txn is SINGLE_HOME, forward to the Sequencer in its home region.
 *         If the txn is MULTI_HOME, forward to the GlobalPaxos for ordering.
 */
class Forwarder : public BasicModule {
public:
  Forwarder(ConfigurationPtr config, Broker& broker);

protected:
  void HandleInternalRequest(
      internal::Request&& req,
      string&& from_machine_id) final;

  void HandleInternalResponse(
      internal::Response&& res,
      string&& from_machine_id) final;

private:
  /**
   * Pre-condition: transaction type is not UNKNOWN
   */
  void Forward(Transaction* txn);

  ConfigurationPtr config_;
  unordered_map<TxnId, Transaction*> pending_transaction_;

  // A constant seed is fine so we don't need a random_device
  std::mt19937 re_;
  std::uniform_int_distribution<> RandomPartition;
};

} // namespace slog