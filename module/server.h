#pragma once

#include <thread>
#include <unordered_map>

#include <zmq.hpp>

#include "module/module.h"

using std::unordered_map;

namespace slog {

class Server : public Module {
public:
  Server(ChannelListener* listener);

  void HandleRequest(const proto::Request& request) final;
  void HandleResponse(const proto::Response& response) final;
  void PostProcessing() final;

private:
  unordered_map<uint32_t, proto::Request> waiting_requests_;
};

} // namespace slog