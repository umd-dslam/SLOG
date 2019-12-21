#pragma once

#include <string>

namespace slog {

using Key = std::string;
using Value = std::string;

enum class ChannelName : size_t {
  SERVER,
  SEQUENCER,
  SCHEDULER,

  END
};

} // namespace slog