#pragma once

#include <chrono>
#include <thread>
#include <set>
#include <random>
#include <unordered_map>

#include <zmq.hpp>

#include "common/configuration.h"
#include "common/types.h"
#include "connection/broker.h"
#include "module/module.h"

using std::unordered_map;

namespace slog {

class Server : public Module {
public:
  Server(
      std::shared_ptr<const Configuration> config,
      std::shared_ptr<zmq::context_t> context,
      Broker& broker);

private:
  void SetUp() final;
  void Loop() final;

  void HandleAPIRequest(MMessage&& msg);
  void HandleInternalRequest(MMessage&& msg);
  void HandleInternalResponse(MMessage&& msg);

  uint32_t NextTxnId();

  std::shared_ptr<const Configuration> config_;
  zmq::socket_t socket_;
  std::unique_ptr<Channel> listener_;
  std::vector<zmq::pollitem_t> poll_items_;

  std::mt19937_64 rand_eng_;
  std::uniform_int_distribution<> dist_;

  uint32_t server_id_;
  uint32_t counter_;
  unordered_map<uint32_t, MMessage> pending_response_;
  std::set<std::pair<TimePoint, uint32_t>> response_time_;
};

} // namespace slog