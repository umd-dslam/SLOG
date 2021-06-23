#pragma once

#include <unordered_map>

#include "module/scheduler_components/remaster_manager.h"

using std::unordered_map;
using std::unordered_set;

namespace slog {

/**
 * Basic, inefficient implimentation of remastering. Transactions are kept in the exact
 * order as their local logs: if a transaction from region 1 is blocked in the queue, all
 * following transactions from region 1 will be blocked behind it
 */
class SimpleRemasterManager : public RemasterManager {
 public:
  SimpleRemasterManager() = default;
  SimpleRemasterManager(const shared_ptr<const Storage>& storage);

  VerifyMasterResult VerifyMaster(Transaction& txn) final;
  RemasterOccurredResult RemasterOccured(const Key& key, uint32_t remaster_counter) final;
  RemasterOccurredResult ReleaseTransaction(const Transaction& txn_holder) final;

  void SetStorage(const shared_ptr<const Storage>& storage) { storage_ = storage; }

 private:
  /**
   * Test if the head of this queue can be unblocked
   */
  void TryToUnblock(uint32_t local_log_machine_id, RemasterOccurredResult& result);

  // Needs access to storage to check counters
  shared_ptr<const Storage> storage_;

  // One queue is kept per local log
  unordered_map<uint32_t, list<Transaction*>> blocked_queue_;
};

}  // namespace slog
