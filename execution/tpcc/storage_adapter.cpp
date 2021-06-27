#include "execution/tpcc/storage_adapter.h"

#include <glog/logging.h>

namespace slog {
namespace tpcc {

KVStorageAdapter::KVStorageAdapter(const std::shared_ptr<Storage>& storage,
                                   const std::shared_ptr<MetadataInitializer>& metadata_initializer)
    : storage_(storage), metadata_initializer_(metadata_initializer) {}

const std::string* KVStorageAdapter::Read(const std::string& key) {
  Record r;
  auto ok = storage_->Read(key, r);
  if (!ok) {
    return nullptr;
  }
  buffer_.push_back(r.to_string());
  return &buffer_.back();
};

bool KVStorageAdapter::Insert(const std::string& key, std::string&& value) {
  Record r(std::move(value));
  r.SetMetadata(metadata_initializer_->Compute(key));
  if (storage_->Write(key, std::move(r))) {
    return false;
  }
  return true;
}

TxnStorageAdapter::TxnStorageAdapter(Transaction& txn) : txn_(txn) {
  for (int i = 0; i < txn.keys_size(); i++) {
    key_index_.emplace(txn.keys(i).key(), i);
  }
}

void TxnStorageAdapter::CheckIndexSize() {
  CHECK_EQ(key_index_.size(), txn_.keys_size()) << "Size of key list in the transaction has changed";
}

const std::string* TxnStorageAdapter::Read(const std::string& key) {
  CheckIndexSize();
  auto it = key_index_.find(key);
  if (it == key_index_.end()) {
    return nullptr;
  }
  return &txn_.keys(it->second).value_entry().value();
}

bool TxnStorageAdapter::Insert(const std::string& key, std::string&& value) {
  CheckIndexSize();
  auto it = key_index_.find(key);
  if (it == key_index_.end()) {
    return false;
  }
  auto value_entry = txn_.mutable_keys(it->second)->mutable_value_entry();
  if (value_entry->type() != KeyType::WRITE) {
    return false;
  }
  value_entry->set_new_value(std::move(value));
  return true;
}

bool TxnStorageAdapter::Update(const std::string& key, std::string&& value) {
  CheckIndexSize();
  auto it = key_index_.find(key);
  if (it == key_index_.end()) {
    return false;
  }
  auto value_entry = txn_.mutable_keys(it->second)->mutable_value_entry();
  if (value_entry->type() != KeyType::WRITE) {
    return false;
  }
  value_entry->set_new_value(std::move(value));
  return true;
}

bool TxnStorageAdapter::Delete(std::string&& key) {
  CheckIndexSize();
  auto it = key_index_.find(key);
  if (it == key_index_.end()) {
    return false;
  }
  for (int i = it->second; i < txn_.keys_size() - 1; i++) {
    txn_.mutable_keys(i)->Swap(txn_.mutable_keys(i + 1));
    key_index_[txn_.keys(i).key()] = i;
  }
  txn_.mutable_keys()->RemoveLast();
  txn_.mutable_deleted_keys()->Add(std::move(key));
  key_index_.erase(it);
  return true;
}

}  // namespace tpcc
}  // namespace slog