#pragma once

#include <cstdint>

namespace slog {

class PaxosClient {
public:
  virtual void Propose(uint32_t value) = 0;
};

} // namespace slog