#include "storage/mem_only_storage.h"

MemOnlyStorage::MemOnlyStorage() {
  table_ = std::make_unique<Table>();
}

bool MemOnlyStorage::Read(const Key& key, Record* result) {
  if (table_->count(key) == 0) {
    return false;
  }
  *result = (*table_)[key];
  return true;
}

void MemOnlyStorage::Write(const Key& key, const Record& record) {
  (*table_)[key] = record;
}

bool MemOnlyStorage::Delete(const Key& key) {
  if (table_->count(key) == 0) {
    return false;
  }
  table_->erase(key);
  return true;
}

bool MemOnlyStorage::GetMasterMetadata(const Key& key, Metadata* result) {
  if (table_->count(key) == 0) {
    return false;
  }
  *result = (*table_)[key].metadata;
  return true;
}