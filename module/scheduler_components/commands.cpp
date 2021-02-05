#include "module/scheduler_components/commands.h"

#include <glog/logging.h>

#include <chrono>
#include <string>
#include <thread>

#include "common/proto_utils.h"
#include "common/string_utils.h"

using std::move;
using std::string;
using std::vector;

namespace slog {

namespace {
const string SPACE(" \t\n\v\f\r");
}  // namespace

const std::unordered_map<string, size_t> KeyValueCommands::COMMAND_NUM_ARGS = {{"GET", 1},  {"SET", 2}, {"DEL", 1},
                                                                               {"COPY", 2}, {"EQ", 2},  {"SLEEP", 1}};

void KeyValueCommands::Execute(Transaction& txn) {
  Reset();
  auto& keys = *txn.mutable_keys();

  // If a command will write to a key but that key is
  // not in the write set, that command will be ignored.
  while (NextCommand(txn.code())) {
    if (cmd_ == "SET") {
      auto it = keys.find(args_[0]);
      if (it == keys.end() || it->second.type() != KeyType::WRITE) {
        continue;
      }
      it->second.set_new_value(move(args_[1]));
    } else if (cmd_ == "DEL") {
      if (!keys.contains(args_[0]) || keys.at(args_[0]).type() != KeyType::WRITE) {
        continue;
      }
      txn.mutable_deleted_keys()->Add(move(args_[0]));
    } else if (cmd_ == "COPY") {
      auto src_it = keys.find(args_[0]);
      auto dst_it = keys.find(args_[1]);
      if (src_it == keys.end()) {
        continue;
      }
      if (dst_it == keys.end() || dst_it->second.type() != KeyType::WRITE) {
        continue;
      }
      dst_it->second.set_new_value(src_it->second.value());
    } else if (cmd_ == "EQ") {
      auto it = keys.find(args_[0]);
      if (it == keys.end()) {
        continue;
      }
      if (it->second.value() != args_[1]) {
        Abort() << "Key = " << args_[0] << ". Expected value = " << args_[1]
                << ". Actual value = " << it->second.value();
      }
    } else if (cmd_ == "SLEEP") {
      std::this_thread::sleep_for(std::chrono::seconds(std::stoi(args_[0])));
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

  auto it = COMMAND_NUM_ARGS.find(cmd_);
  if (it == COMMAND_NUM_ARGS.end()) {
    Abort() << "Invalid command: " << cmd_;
    return false;
  }

  auto required_num_args = it->second;
  pos_ = NextNTokens(args_, code, SPACE, required_num_args, pos_);
  if (pos_ == string::npos) {
    Abort() << "Invalid number of arguments for command " << cmd_;
    return false;
  }

  return true;
}

void TPCCCommands::Execute(Transaction& /*txn*/) {}

}  // namespace slog