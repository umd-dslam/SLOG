#pragma once

namespace slog {

template <typename K, typename M>
class LookupMasterIndex {
public:
  virtual bool GetMasterMetadata(const K& key, M& metadata) const = 0;
};

} // namespace slog