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
  string MakeEndpoint(const string& addr = "") const;

  /**
   * All brokers only start working after every other broker is up and send a READY
   * message to everyone. There is one caveat: if after the synchronization happens, 
   * a machine goes down, and restarts, that machine cannot join anymore since the
   * READY messages are only sent once in the beginning. 
   * In production system, HEARTBEAT messages should be periodically sent out instead
   * to mitigate this problem.
   */
  bool InitializeConnection();
  
  void Run();

  shared_ptr<Configuration> config_;
  zmq::socket_t router_;
  std::atomic<bool> running_;

  std::thread thread_;

  vector<unique_ptr<Channel>> channels_;

  // Map from ip addresses to sockets
  unordered_map<string, unique_ptr<zmq::socket_t>> address_to_socket_;
  // Map from connection ids (zmq identities) to serialized-to-string SlogIdentifiers
  // Used to translate the identities of incoming messages
  unordered_map<string, string> connection_id_to_slog_id_;
  // Map from serialized-to-string SlogIdentifiers to IP addresses
  // Used to translate the identities of outgoing messages
  unordered_map<string, string> slog_id_to_address_;
};

} // namespace slog