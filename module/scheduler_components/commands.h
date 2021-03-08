#pragma once

#include <chrono>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/configuration.h"
#include "common/string_utils.h"
#include "common/types.h"
#include "proto/transaction.pb.h"
#include "storage/storage.h"

namespace slog {

template <typename K, typename R>
class Commands {
 public:
  virtual ~Commands() = default;

  virtual void Execute(Transaction& txn) = 0;
};

template <typename K, typename R>
class KeyValueCommands : public Commands<K, R> {
  const static std::string kSpace;
  const static std::unordered_map<std::string, size_t> kCommands;

  ConfigurationPtr config_;
  std::shared_ptr<Storage<K, R>> storage_;

 public:
  KeyValueCommands(const ConfigurationPtr& config, const std::shared_ptr<Storage<K, R>>& storage) 
    : config_(config), storage_(storage) {}

  void Execute(Transaction& txn) final {
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

      for (const auto& [key, value] : txn.keys()) {
        if (!config_->key_is_in_local_partition(key) || value.type() == KeyType::READ) {
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

 private:
  void Reset() {
    pos_ = 0;
    aborted_ = false;
    abort_reason_.clear();
    abort_reason_.str(std::string());
  }

  std::ostringstream& Abort() {
    aborted_ = true;
    return abort_reason_;
  }

  bool NextCommand(const std::string& code) {
    pos_ = NextToken(cmd_, code, kSpace, pos_);
    if (pos_ == std::string::npos) {
      return false;
    }

    auto it = kCommands.find(cmd_);
    if (it == kCommands.end()) {
      Abort() << "Invalid command: " << cmd_;
      return false;
    }

    auto required_num_args = it->second;
    pos_ = NextNTokens(args_, code, kSpace, required_num_args, pos_);
    if (pos_ == std::string::npos) {
      Abort() << "Invalid number of arguments for command " << cmd_;
      return false;
    }

    return true;
  }

  std::string cmd_;
  std::vector<std::string> args_;
  size_t pos_;
  bool aborted_;
  std::ostringstream abort_reason_;
};

template <typename K, typename R>
const std::string KeyValueCommands<K, R>::kSpace(" \t\n\v\f\r");
template <typename K, typename R>
const std::unordered_map<std::string, size_t> KeyValueCommands<K, R>::kCommands = {
    {"GET", 1}, {"SET", 2}, {"DEL", 1}, {"COPY", 2}, {"EQ", 2}, {"SLEEP", 1}};

template <typename K, typename R>
class DummyCommands : public Commands<K, R> {
  ConfigurationPtr config_;
  std::shared_ptr<Storage<K, R>> storage_;

 public:
  DummyCommands(const ConfigurationPtr& config, const std::shared_ptr<Storage<K, R>>& storage) 
    : config_(config), storage_(storage) {}

  void Execute(Transaction& txn) final {
    // This loop comes from the old SLOG implementation
    for (const auto& [key, value] : txn.keys()) {
      if (!config_->key_is_in_local_partition(key) || value.type() == KeyType::READ) {
        continue;
      }
      if (auto record = storage_->Read(key); record != nullptr) {
        for (size_t j = 0; j < std::min((size_t)8, record->size()); j++) {
          auto data = record->data();
          if (data[j] + 1 > 'z') {
            data[j] = 'a';
          } else {
            data[j] = data[j] + 1;
          }
        }
      }
    }
    txn.set_status(TransactionStatus::COMMITTED);
  }
};

template <typename K, typename R>
class NoopCommands : public Commands<K, R> {
 public:
  void Execute(Transaction& txn) final {
    txn.set_status(TransactionStatus::COMMITTED);
  }
};

}  // namespace slog