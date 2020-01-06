#pragma once

#include <string>

#include <zmq.hpp>

#include "module/module.h"

using std::string;

namespace slog {

class WorkloadGenerator : public Module {
public:
  WorkloadGenerator(
      std::shared_ptr<zmq::context_t> context,
      const std::string& host, 
      uint32_t port);

private:
  void SetUp() final;
  void Loop() final;

  zmq::socket_t socket_;
  string host_;
  uint32_t port_;
};

} // namespace slog