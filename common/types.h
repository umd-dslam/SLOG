#pragma once

#include <chrono>
#include <string>

using namespace std::chrono;

namespace slog {

using Key = std::string;
using Value = std::string;

using Clock = steady_clock;
using TimePoint = Clock::time_point;
using Duration = Clock::duration;

} // namespace slog