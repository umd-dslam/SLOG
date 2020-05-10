#include "module/scheduler_components/abort_manager.h"

#include "module/base/basic_module.h"
#include "common/proto_utils.h"
#include "proto/internal.pb.h"

#include <glog/logging.h>

namespace slog {

using internal::Request;

AbortManager::AbortManager(ConfigurationPtr config, shared_ptr<TransactionMap> all_txns) :
    config_(config), all_txns_(all_txns) {}

vector<TxnId> AbortManager::AbortTransaction(const TransactionHolder* txn_holder, bool was_dispatched) {
  vector<TxnId> can_abort;
  CHECK(txn_holder->InvolvedPartitions().size() == 1) << "MP not impelemented";

  auto txn = txn_holder->GetTransaction();
  switch (txn->internal().type()) {
    case TransactionType::SINGLE_HOME: {
      txn->set_status(TransactionStatus::ABORTED);
      can_abort.push_back(txn->internal().id());
      break;
    }
    case TransactionType::MULTI_HOME: {
      break;
    }
    case TransactionType::LOCK_ONLY: {
      break;
    }
    default:
      LOG(ERROR) << "Unknown transaction type";
      break;
  }
  return can_abort;
}

}