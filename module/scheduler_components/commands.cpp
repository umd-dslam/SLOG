#include "module/scheduler_components/commands.h"

#include <glog/logging.h>

#include "common/proto_utils.h"
#include "common/string_utils.h"

using std::string;
using std::vector;

namespace slog {

namespace {
const string SPACE(" \t\n\v\f\r");
} // namespace

const std::unordered_map<string, size_t>
KeyValueCommands::COMMAND_NUM_ARGS = {
  {"GET", 1}, {"SET", 2}, {"DEL", 1}, {"COPY", 2}, {"ABORT", 1}
};

void KeyValueCommands::Execute(Transaction& txn) {
  Reset();
  auto& read_set = *txn.mutable_read_set();
  auto& write_set = *txn.mutable_write_set();
  auto& delete_set = *txn.mutable_delete_set();

  while (NextCommand(txn.code())) {
    if (cmd_ == "SET") {
      if (write_set.contains(args_[0])) {
        write_set[args_[0]] = std::move(args_[1]);
      }
    } else if (cmd_ == "DEL") {
      if (write_set.contains(args_[0])) {
        delete_set.Add(std::move(args_[0]));
      }
    } else if (cmd_ == "COPY") {
      const auto& src = args_[0];
      const auto& dst = args_[1];
      if (read_set.contains(src) && write_set.contains(dst)) {
        write_set[dst] = read_set.at(src);
      }
    } else if (cmd_ == "ABORT") {
      Abort() << "User abort (key: " << args_[0] << ")";
    }
  }

  if (aborted_) {
    txn.set_status(TransactionStatus::ABORTED);
    txn.set_abort_reason(abort_reason_.str());
  } else {
    txn.set_status(TransactionStatus::COMMITTED);
  }
}

void KeyValueCommands::Reset() {
  pos_ = 0;
  aborted_ = false;
  abort_reason_.clear();
  abort_reason_.str(string());
}

std::ostringstream& KeyValueCommands::Abort() {
  aborted_ = true;
  return abort_reason_;
}

bool KeyValueCommands::NextCommand(const string& code) {
  pos_ = NextToken(cmd_, code, SPACE, pos_);
  if (pos_ == string::npos) {
    return false;
  }

  if (COMMAND_NUM_ARGS.count(cmd_) == 0) {
    Abort() << "Invalid command: " << cmd_; 
    return false;
  }

  auto required_num_args = COMMAND_NUM_ARGS.at(cmd_);
  pos_ = NextNTokens(args_, code, SPACE, required_num_args, pos_);
  if (pos_ == string::npos) {
    Abort() << "Invalid number of arguments for command " << cmd_;
    return false;
  }

  return true;
}

void TPCCCommands::Execute(Transaction& /*txn*/) {
  
}

} // namespace slog