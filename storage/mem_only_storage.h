#pragma once

#include <memory>
#include <unordered_map>
#include "storage/storage.h"
#include "storage/lookup_master_index.h"

namespace slog {

template<typename K, typename R, typename M>
class MemOnlyStorage : 
    public Storage<K, R>,
    public LookupMasterIndex<K, M> {
public:

  bool Read(const K& key, R& result) const final {
    if (table_.count(key) == 0) {
      return false;
    }
    result = table_.at(key);
    return true;
  }

  void Write(const K& key, const R& record) final {
    table_[key] = record;
  }

  bool Delete(const K& key) final {
    if (table_.count(key) == 0) {
      return false;
    }
    table_.erase(key);
    return true;
  }

  bool GetMasterMetadata(const K& key, M& metadata) const final {
    if (table_.count(key) == 0) {
      return false;
    }
    metadata = table_.at(key).metadata;
    return true;
  }

private:
  std::unordered_map<K, R> table_;
};

} // namespace slog