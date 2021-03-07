#pragma once

#include "data_structure/concurrent_hash_map.h"

#include "storage/storage.h"
#include "storage/lookup_master_index.h"

namespace slog {

template<typename K, typename R, typename M>
class MemOnlyStorage : 
    public Storage<K, R>,
    public LookupMasterIndex<K, M> {
public:
  bool Read(const K& key, R& result) const final {
    return table_.Get(result, key);
  }

  R* Read(const K& key) final {
    return table_.GetUnsafe(key);
  }

  void Write(const K& key, const R& record) final {
    table_.InsertOrUpdate(key, record);
  }

  bool Delete(const K& key) final {
    return table_.Erase(key);
  }

  bool GetMasterMetadata(const K& key, M& metadata) const final {
    R rec;
    if (!table_.Get(rec, key)) {
      return false;
    }
    metadata = rec.metadata;
    return true;
  }

private:
  ConcurrentHashMap<K, R> table_;
};

} // namespace slog