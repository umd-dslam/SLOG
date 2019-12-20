#pragma once

#include <thread>
#include <unordered_map>

#include <zmq.hpp>

#include "common/configuration.h"
#include "connection/channel.h"

using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;

namespace slog {
class Broker {
public:
  static const std::string SERVER_CHANNEL;

  Broker(
      shared_ptr<Configuration> config, 
      shared_ptr<zmq::context_t> context);
  ~Broker();

  ChannelListener* GetChannelListener(const string& name);

private:
  void InitializeConnection();
  void Run();

  shared_ptr<Configuration> config_;
  zmq::socket_t router_;
  std::atomic<bool> running_;

  std::thread thread_;

  vector<unique_ptr<Channel>> channels_;
  unordered_map<string, unique_ptr<zmq::socket_t>> address_to_socket_;
  unordered_map<string, string> connection_id_to_slog_id_;
  unordered_map<string, string> slog_id_to_address_;
};

} // namespace slog