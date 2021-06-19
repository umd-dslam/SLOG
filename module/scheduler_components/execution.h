#pragma once

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
    auto& keys = *txn.mutable_keys();
    bool aborted = false;
    std::ostringstream abort_reason;
    for (auto& p : txn.code().procedures()) {
      const auto& args = p.args();
      if (args.empty()) {
        continue;
      }
      if (args[0] == "SET") {
        auto it = keys.find(args[1]);
        if (it == keys.end() || it->second.type() != KeyType::WRITE) {
          continue;
        }
        it->second.set_new_value(args[2]);
      } else if (args[0] == "DEL") {
        if (!keys.contains(args[1]) || keys.at(args[1]).type() != KeyType::WRITE) {
          continue;
        }
        txn.mutable_deleted_keys()->Add(std::string(args[1]));
      } else if (args[0] == "COPY") {
        auto src_it = keys.find(args[1]);
        auto dst_it = keys.find(args[2]);
        if (src_it == keys.end()) {
          continue;
        }
        if (dst_it == keys.end() || dst_it->second.type() != KeyType::WRITE) {
          continue;
        }
        dst_it->second.set_new_value(src_it->second.value());
      } else if (args[0] == "EQ") {
        auto it = keys.find(args[1]);
        if (it == keys.end()) {
          continue;
        }
        if (it->second.value() != args[2]) {
          aborted = true;
          abort_reason << "Key = " << args[1] << ". Expected value = " << args[2]
                       << ". Actual value = " << it->second.value();
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

      for (const auto& [key, value] : txn.keys()) {
        if (!sharder_->is_local_key(key) || value.type() == KeyType::READ) {
          continue;
        }
        if (auto record = storage_->Read(key); record != nullptr) {
          record->metadata = value.metadata();
          record->SetValue(value.new_value());
        } else {
          R new_record;
          new_record.metadata = value.metadata();
          new_record.SetValue(value.new_value());
          storage_->Write(key, new_record);
        }
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