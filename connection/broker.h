#pragma once

#include <thread>
#include <zmq.hpp>

#include "common/configuration.h"

namespace slog {

class Broker {
public:
  static const std::string SERVER_ENDPOINT;
  static const std::string SEQUENCER_ENDPOINT;
  static const std::string SCHEDULER_ENDPOINT;

  Broker(std::shared_ptr<Configuration> config, zmq::context_t& context);
  ~Broker();

  void Run();

private:
  std::shared_ptr<Configuration> config_;

  zmq::socket_t router_;
  std::thread thread_;

  zmq::socket_t server_channel_;
  zmq::socket_t sequencer_channel_;
  zmq::socket_t scheduler_channel_;

  std::atomic<bool> running_;
};

} // namespace slog