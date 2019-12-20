#pragma once

#include <string>

namespace slog {

using Key = std::string;
using Value = std::string;

struct MachineIdentifier {
  uint32_t replica;
  uint32_t partition;
  
  MachineIdentifier(uint32_t replica, uint32_t partition)
    : replica(replica), 
      partition(partition) {}
};

} // namespace slog