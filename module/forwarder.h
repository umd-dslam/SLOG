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

class Forwarder : public BasicModule {
public:
  Forwarder(
      shared_ptr<Configuration> config,
      Broker& broker);

protected:
  void HandleInternalRequest(
      internal::Request&& req,
      string&& from_machine_id) final;

  void HandleInternalResponse(
      internal::Response&& res,
      string&& from_machine_id) final;

private:
  internal::Request MakeLookupMasterRequest(const Transaction& txn);

  /**
   * Pre-condition: transaction type is not UNKNOWN
   */
  void Forward(Transaction* txn);

  shared_ptr<Configuration> config_;
  unordered_map<TxnId, Transaction*> pending_transaction_;

  // A constant seed is fine so we don't need a random_device
  std::mt19937 re_;
  std::uniform_int_distribution<> RandomPartition;
};

} // namespace slog