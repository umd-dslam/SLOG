#pragma once

#include <string>

namespace slog {

const long BROKER_POLL_TIMEOUT_MS = 100;
const long SERVER_POLL_TIMEOUT_MS = 100;

const std::string SERVER_CHANNEL("server");
const std::string WORKLOAD_CHANNEL("workload");

const uint32_t MAX_TXN_COUNT = 1000000;

} // namespace slog