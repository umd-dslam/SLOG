#pragma once

#include "common/types.h"
#include "common/transaction_holder.h"
#include "common/configuration.h"

using std::shared_ptr;

namespace slog {

using TransactionMap = unordered_map<TxnId, TransactionHolder>;

class AbortManager {
public:
  AbortManager(ConfigurationPtr config, shared_ptr<TransactionMap> all_txns);
  void ShouldAbort(const TransactionHolder* txn_holder);
  vector<TxnId> AbortTransaction(const TransactionHolder* txn_holder, bool was_dispatched);
private:
  ConfigurationPtr config_;
  shared_ptr<TransactionMap> all_txns_;

  // The number of lock-only transactions that have been collected for an abort
  unordered_map<TxnId, uint32_t> abort_waits_on_;
};

} // namespace slog
