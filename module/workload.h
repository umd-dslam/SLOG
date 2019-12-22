#pragma once

#include <chrono>
#include <thread>
#include <set>
#include <unordered_map>

#include <zmq.hpp>

#include "common/types.h"
#include "module/module.h"

using std::unordered_map;

namespace slog {

class Workload : public Module {
public:
  Workload(ChannelListener* listener);

  void HandleMessage(MMessage message) final;

  void PostProcessing() final;

private:

  std::mt19937_64 rand_eng_;
  std::uniform_int_distribution<> dist_;

  unordered_map<uint32_t, MMessage> waiting_requests_;
  std::set<std::pair<TimePoint, uint32_t>> response_time_;
};

} // namespace slog