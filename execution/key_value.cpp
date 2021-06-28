#include <sstream>
#include <thread>

#include "execution/execution.h"

namespace slog {

KeyValueExecution::KeyValueExecution(const SharderPtr& sharder, const std::shared_ptr<Storage>& storage)
    : sharder_(sharder), storage_(storage) {}

void KeyValueExecution::Execute(Transaction& txn) {
  bool aborted = false;
  std::ostringstream abort_reason;
  std::unordered_map<std::string, int> key_index;
  for (int i = 0; i < txn.keys_size(); i++) {
    key_index.emplace(txn.keys(i).key(), i);
  }

  for (auto& p : txn.code().procedures()) {
    const auto& args = p.args();
    if (args.empty()) {
      continue;
    }
    if (args[0] == "SET") {
      auto it = key_index.find(args[1]);
      if (it == key_index.end()) {
        continue;
      }
      auto value = txn.mutable_keys(it->second)->mutable_value_entry();
      if (value->type() != KeyType::WRITE) {
        continue;
      }
      value->set_new_value(args[2]);
    } else if (args[0] == "DEL") {
      auto it = key_index.find(args[1]);
      if (it != key_index.end() || txn.keys(it->second).value_entry().type() != KeyType::WRITE) {
        continue;
      }
      txn.mutable_deleted_keys()->Add(std::string(args[1]));
    } else if (args[0] == "COPY") {
      auto src_it = key_index.find(args[1]);
      auto dst_it = key_index.find(args[2]);
      if (src_it == key_index.end() || dst_it == key_index.end()) {
        continue;
      }
      const auto& src_value = txn.keys(src_it->second).value_entry();
      auto dst_value = txn.mutable_keys(dst_it->second)->mutable_value_entry();
      if (dst_value->type() != KeyType::WRITE) {
        continue;
      }
      dst_value->set_new_value(src_value.value());
    } else if (args[0] == "EQ") {
      auto it = key_index.find(args[1]);
      if (it == key_index.end()) {
        continue;
      }
      const auto& value = txn.keys(it->second).value_entry().value();
      if (value != args[2]) {
        aborted = true;
        abort_reason << "Key = " << args[1] << ". Expected value = " << args[2] << ". Actual value = " << value;
      }
    } else if (args[0] == "SLEEP") {
      std::this_thread::sleep_for(std::chrono::milliseconds(std::stoi(args[1])));
    }
  }

  if (aborted) {
    txn.set_status(TransactionStatus::ABORTED);
    txn.set_abort_reason(abort_reason.str());
  } else {
    txn.set_status(TransactionStatus::COMMITTED);
    ApplyWrites(txn, sharder_, storage_);
  }
}

}  // namespace slog