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
  void FillLookupMasterRequest(internal::Request& req, const Transaction& txn);

  /**
   * Pre-condition: transaction type is not UNKNOWN
   */
  void Forward(const Transaction& txn);

  shared_ptr<Configuration> config_;
  unordered_map<TxnId, Transaction> pending_transaction_;

  std::random_device rd_;
  std::mt19937 re_;
};

} // namespace slog