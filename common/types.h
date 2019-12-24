#pragma once

#include <chrono>
#include <string>

using namespace std::chrono;

namespace slog {

using Key = std::string;
using Value = std::string;

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

using Clock = steady_clock;
using TimePoint = Clock::time_point;
using Duration = Clock::duration;

} // namespace slog