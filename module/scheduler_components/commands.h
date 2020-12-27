#pragma once

#include <sstream>
#include <unordered_map>
#include <vector>

#include "common/types.h"
#include "proto/transaction.pb.h"

namespace slog {

class Commands {
 public:
  virtual ~Commands() = default;
  virtual void Execute(Transaction& txn) = 0;
};

class KeyValueCommands : public Commands {
 public:
  void Execute(Transaction& txn) final;

 private:
  static const std::unordered_map<std::string, size_t> COMMAND_NUM_ARGS;

  void Reset();
  std::ostringstream& Abort();
  bool NextCommand(const std::string& code);

  std::string cmd_;
  std::vector<std::string> args_;
  size_t pos_;
  bool aborted_;
  std::ostringstream abort_reason_;
};

class TPCCCommands : public Commands {
 public:
  void Execute(Transaction& txn) final;
};

}  // namespace slog