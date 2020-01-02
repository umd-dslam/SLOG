#pragma once

#include <cstdint>

namespace slog {

class PaxosClient {
public:
  virtual void Append(uint32_t value);
};

} // namespace slog