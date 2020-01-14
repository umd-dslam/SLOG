#pragma once

#include "common/types.h"
#include "proto/transaction.pb.h"

using std::shared_ptr;

namespace slog {

class StoredProcedures {
public:
  virtual void Execute(Transaction& txn) = 0;
};

class KeyValueStoredProcedures : public StoredProcedures {
public:
  void Execute(Transaction& txn) final;
};

class TPCCStoredProcedures : public StoredProcedures {
public:
  void Execute(Transaction& txn) final;
};

} // namespace slog