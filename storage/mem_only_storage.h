#pragma once

#include <memory>
#include <unordered_map>
#include "storage/storage.h"
#include "common/types.h"

namespace slog {

struct Metadata {
  Metadata(uint32_t m, uint32_t c = 0) : master(m), counter(c) {}
  Metadata() = default;

  uint32_t master;
  uint32_t counter;
};

struct Record {
  Record(Value v, uint32_t m, uint32_t c = 0) : value(v), metadata(m, c) {}
  Record() = default;

  Value value;
  Metadata metadata;
};

using Table = std::unordered_map<Key, Record>;

class MemOnlyStorage : Storage<Key, Record, Metadata> {
public:
  MemOnlyStorage();

  bool Read(const Key& key, Record* result) const;

  void Write(const Key& key, const Record& record);

  bool Delete(const Key& key);

  bool GetMasterMetadata(const Key& key, Metadata* metadata) const;

private:
  std::unique_ptr<Table> table_;
};

} // namespace slog