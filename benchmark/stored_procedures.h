#pragma once

#include <sstream>
#include <unordered_map>
#include <vector>

#include "common/types.h"
#include "proto/transaction.pb.h"

using std::pair;
using std::shared_ptr;
using std::string;

namespace slog {

class StoredProcedures {
public:
  virtual void Execute(Transaction& txn) = 0;
};

class KeyValueStoredProcedures : public StoredProcedures {
public:
  void Execute(Transaction& txn) final;

private:
  static const std::unordered_map<string, size_t> COMMAND_NUM_ARGS;

  void Reset();
  std::ostringstream& Abort();
  bool NextCommand(const string& code);

  string cmd_;
  std::vector<string> args_;
  size_t pos_;
  bool aborted_;
  std::ostringstream abort_reason_;
};

class TPCCStoredProcedures : public StoredProcedures {
public:
  void Execute(Transaction& txn) final;
};

} // namespace slog