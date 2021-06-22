#pragma once

#include <unordered_map>

#include "common/sharder.h"
#include "proto/transaction.pb.h"
#include "storage/storage.h"

namespace slog {

class Execution {
 public:
  virtual void Execute(Transaction& txn) = 0;
};

template <typename K, typename R>
class KeyValueExecution : public Execution {
  SharderPtr sharder_;
  std::shared_ptr<Storage<K, R>> storage_;

 public:
  KeyValueExecution(const SharderPtr& sharder, const std::shared_ptr<Storage<K, R>>& storage)
      : sharder_(sharder), storage_(storage) {}

  void Execute(Transaction& txn) final {
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

      for (const auto& kv : txn.keys()) {
        const auto& key = kv.key();
        const auto& value = kv.value_entry();
        if (!sharder_->is_local_key(key) || value.type() == KeyType::READ) {
          continue;
        }
        R new_record;
        new_record.SetMetadata(value.metadata());
        new_record.SetValue(value.new_value());
        storage_->Write(key, new_record);
      }
      for (const auto& key : txn.deleted_keys()) {
        storage_->Delete(key);
      }
    }
  }
};

class NoopExecution : public Execution {
 public:
  void Execute(Transaction& txn) final { txn.set_status(TransactionStatus::COMMITTED); }
};

}  // namespace slog