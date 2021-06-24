#include "execution/execution.h"

namespace slog {

TPCCExecution::TPCCExecution(const SharderPtr& sharder, const std::shared_ptr<Storage>& storage)
    : sharder_(sharder), storage_(storage) {}

void TPCCExecution::Execute(Transaction& txn) { txn.set_status(TransactionStatus::COMMITTED); }

}  // namespace slog