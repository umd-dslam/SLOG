#pragma once

#include <unordered_map>

#include "common/sharder.h"
#include "execution/execution.h"
#include "proto/transaction.pb.h"
#include "storage/storage.h"

namespace slog {

class Execution {
 public:
  virtual ~Execution() = default;
  virtual void Execute(Transaction& txn) = 0;

  static void ApplyWrites(const Transaction& txn, const SharderPtr& sharder, const std::shared_ptr<Storage>& storage);
};

class KeyValueExecution : public Execution {
 public:
  KeyValueExecution(const SharderPtr& sharder, const std::shared_ptr<Storage>& storage);
  void Execute(Transaction& txn) final;

 private:
  SharderPtr sharder_;
  std::shared_ptr<Storage> storage_;
};

class NoopExecution : public Execution {
 public:
  void Execute(Transaction& txn) final { txn.set_status(TransactionStatus::COMMITTED); }
};

class TPCCExecution : public Execution {
 public:
  TPCCExecution(const SharderPtr& sharder, const std::shared_ptr<Storage>& storage);
  void Execute(Transaction& txn) final;

 private:
  SharderPtr sharder_;
  std::shared_ptr<Storage> storage_;
};

}  // namespace slog