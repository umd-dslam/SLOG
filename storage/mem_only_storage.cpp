#include "storage/mem_only_storage.h"

namespace slog {

bool MemOnlyStorage::Read(const Key& key, Record* result) const {
  if (table_.count(key) == 0) {
    return false;
  }
  *result = table_.at(key);
  return true;
}

void MemOnlyStorage::Write(const Key& key, const Record& record) {
  table_[key] = record;
}

bool MemOnlyStorage::Delete(const Key& key) {
  if (table_.count(key) == 0) {
    return false;
  }
  table_.erase(key);
  return true;
}

bool MemOnlyStorage::GetMasterMetadata(const Key& key, Metadata& result) const {
  if (table_.count(key) == 0) {
    return false;
  }
  result = table_.at(key).metadata;
  return true;
}
} // namespace slog