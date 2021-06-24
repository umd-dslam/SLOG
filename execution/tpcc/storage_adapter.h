#pragma once

#include "common/types.h"
#include "proto/transaction.pb.h"
#include "storage/metadata_initializer.h"
#include "storage/storage.h"

namespace slog {
namespace tpcc {

class StorageAdapter {
 public:
  struct UpdateEntry {
    size_t offset;
    size_t size;
    const void* data;
  };

  virtual ~StorageAdapter() = default;
  virtual const std::string* Read(const std::string& key) = 0;
  virtual bool Insert(const std::string& key, std::string&& value) = 0;
  virtual bool Update(const std::string& key, const std::vector<UpdateEntry>& updates) = 0;
  virtual bool Delete(std::string&& key) = 0;
};

class StorageInitializingAdapter : public StorageAdapter {
 public:
  StorageInitializingAdapter(const std::shared_ptr<Storage>& storage,
                             const std::shared_ptr<MetadataInitializer>& metadata_initializer);
  const std::string* Read(const std::string&) override {
    throw std::runtime_error("Read is unimplemented in StorageInitializingAdapter");
  }
  bool Insert(const std::string& key, std::string&& value) override;
  bool Update(const std::string&, const std::vector<UpdateEntry>&) override {
    throw std::runtime_error("Update is unimplemented in StorageInitializingAdapter");
  }
  bool Delete(std::string&&) override {
    throw std::runtime_error("Delete is unimplemented in StorageInitializingAdapter");
  }

 private:
  std::shared_ptr<Storage> storage_;
  std::shared_ptr<MetadataInitializer> metadata_initializer_;
};

class TxnStorageAdapter : public StorageAdapter {
 public:
  TxnStorageAdapter(Transaction& txn);
  const std::string* Read(const std::string& key) override;
  bool Insert(const std::string& key, std::string&& value) override;
  bool Update(const std::string& key, const std::vector<UpdateEntry>& updates) override;
  bool Delete(std::string&& key) override;

 private:
  void CheckIndexSize();
  Transaction& txn_;
  std::unordered_map<std::string, int> key_index_;
};

}  // namespace tpcc
}  // namespace slog