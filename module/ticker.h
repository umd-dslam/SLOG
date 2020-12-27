#pragma once

#include <zmq.hpp>

#include "common/types.h"
#include "module/base/module.h"

namespace slog {

class Ticker : public Module {
 public:
  const static std::string ENDPOINT;
  static zmq::socket_t Subscribe(zmq::context_t& context);

  Ticker(zmq::context_t& context, std::chrono::milliseconds tick_period_ms);
  Ticker(zmq::context_t& context, uint32_t ticks_per_sec);

  void SetUp() final;
  void Loop() final;

 private:
  zmq::socket_t socket_;
  Duration sleep_us_;
};

}  // namespace slog