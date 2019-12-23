#pragma once

#include <string>

namespace slog {

const std::string SERVER_CHANNEL("server");
const std::string WORKLOAD_CHANNEL("workload");

const uint32_t MAX_TXN_COUNT = 1000000;

} // namespace slog