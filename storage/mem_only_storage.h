#pragma once

#include "data_structure/concurrent_hash_map.h"
#include "storage/lookup_master_index.h"
#include "storage/storage.h"

namespace slog {

class MemOnlyStorage : public Storage, public LookupMasterIndex {
 public:
  bool Read(const Key& key, Record& result) const final { return table_.Get(result, key); }

  bool Write(const Key& key, const Record& record) final { return table_.InsertOrUpdate(key, record); }

  bool Delete(const Key& key) final { return table_.Erase(key); }

  bool GetMasterMetadata(const Key& key, Metadata& metadata) const final {
    Record rec;
    if (!table_.Get(rec, key)) {
      return false;
    }
    metadata = rec.metadata();
    return true;
  }

 private:
  ConcurrentHashMap<Key, Record> table_;
};

}  // namespace slog