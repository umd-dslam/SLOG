#pragma once

#include <memory>
#include <unordered_map>
#include "common/types.h"
#include "storage/storage.h"
#include "storage/lookup_master_index.h"

namespace slog {

using Table = std::unordered_map<Key, Record>;

class MemOnlyStorage : 
    public Storage<Key, Record>,
    public LookupMasterIndex<Key, Metadata> {
public:

  bool Read(const Key& key, Record* result) const final;

  void Write(const Key& key, const Record& record) final;

  bool Delete(const Key& key) final;

  bool GetMasterMetadata(const Key& key, Metadata& metadata) const final;

private:
  Table table_;
};

} // namespace slog