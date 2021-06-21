#pragma once

namespace slog {

template <typename K, typename R>
class Storage {
public:
  virtual bool Read(const K& key, R& result) const = 0;

  virtual void Write(const K& key, const R& record) = 0;

  virtual bool Delete(const K& key) = 0;
};

} // namespace slog