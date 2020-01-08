#pragma once

#include <zmq.hpp>

#include "module/base/module.h"

namespace slog {

class Worker : public Module {
public:
  Worker(zmq::context_t& context);
  void SetUp() final;
  void Loop() final;

private:
  zmq::socket_t socket_;
};

} // namespace slog