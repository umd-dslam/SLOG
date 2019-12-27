#pragma once

#include <string>

namespace slog {

const long BROKER_POLL_TIMEOUT_MS = 100;
const long SERVER_POLL_TIMEOUT_MS = 100;

const std::string SERVER_CHANNEL("server");
const std::string FORWARDER_CHANNEL("forwarder");
const std::string SEQUENCER_CHANNEL("sequencer");
const std::string SCHEDULER_CHANNEL("scheduler");

const uint32_t MAX_TXN_COUNT = 1000000;

const size_t MM_PROTO = 0;
const size_t MM_FROM_CHANNEL = 1;
const size_t MM_TO_CHANNEL = 2;

} // namespace slog