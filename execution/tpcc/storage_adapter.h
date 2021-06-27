#pragma once

#include "common/types.h"
#include "proto/transaction.pb.h"
#include "storage/metadata_initializer.h"
#include "storage/storage.h"

namespace slog {
namespace tpcc {

class StorageAdapter {
 public:
  virtual ~StorageAdapter() = default;
  virtual const std::string* Read(const std::string& key) = 0;
  virtual bool Insert(const std::string& key, std::string&& value) = 0;
  virtual bool Update(const std::string& key, std::string&& value) = 0;
  virtual bool Delete(std::string&& key) = 0;
};

using StorageAdapterPtr = std::shared_ptr<StorageAdapter>;

class KVStorageAdapter : public StorageAdapter {
 public:
  KVStorageAdapter(const std::shared_ptr<Storage>& storage,
                   const std::shared_ptr<MetadataInitializer>& metadata_initializer);
  // This Read method is leaky. Only used for testing
  const std::string* Read(const std::string&) override;
  bool Insert(const std::string& key, std::string&& value) override;
  bool Update(const std::string&, std::string&&) override {
    throw std::runtime_error("Update is unimplemented in KVStorageAdapter");
  }
  bool Delete(std::string&&) override { throw std::runtime_error("Delete is unimplemented in KVStorageAdapter"); }

 private:
  std::shared_ptr<Storage> storage_;
  std::shared_ptr<MetadataInitializer> metadata_initializer_;
  std::vector<std::string> buffer_;
};

class TxnStorageAdapter : public StorageAdapter {
 public:
  TxnStorageAdapter(Transaction& txn);
  const std::string* Read(const std::string& key) override;
  bool Insert(const std::string& key, std::string&& value) override;
  bool Update(const std::string& key, std::string&& value) override;
  bool Delete(std::string&& key) override;

 private:
  void CheckIndexSize();
  Transaction& txn_;
  std::unordered_map<std::string, int> key_index_;
};

}  // namespace tpcc
}  // namespace slog