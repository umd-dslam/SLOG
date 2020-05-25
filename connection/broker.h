#pragma once

#include <condition_variable>
#include <thread>
#include <unordered_map>

#include <zmq.hpp>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/mmessage.h"

using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;

namespace slog {

/**
 * A Broker distributes messages into and out of a machine.
 * It runs its own thread with the components depicted below
 * 
 *                   -----------------------
 *                   |                     |
 *  Module A <---> Channel A             Router  <----- Incoming Message
 *                   |         B           |
 *                   |          R          |
 *  Module B <---> Channel B     O         |
 *                   |            K      Dealer  -----> Outgoing Message to XXX.XXX.XXX.XXX
 *                   |             E     Dealer  -----> Outgoing Message to YYY.YYY.YYY.YYY
 *  Module C <---> Channel C        R    Dealer  -----> Outgoing Message to ZZZ.ZZZ.ZZZ.ZZZ
 *                   |                    ....                      ....
 *                   |                     |
 *                   -----------------------
 * 
 * To receive messages from other machines, it uses a ZMQ_ROUTER socket, which automatically
 * prepends a connection identity to an arriving zmq message. Using this identity, it can
 * tell where the message comes from.
 * 
 * The messages going into the system via the router will be brokered to the channel
 * specified in each message. On the other end of each channel is a module which also runs
 * in its own thread. A module can also send messages back to the broker and the broker
 * will subsequently send them to other machines through the appropriate ZMQ_DEALER sockets.
 * 
 */
class Broker {
public:
  Broker(
      const ConfigurationPtr& config, 
      const shared_ptr<zmq::context_t>& context,
      long poll_timeout_ms = BROKER_POLL_TIMEOUT_MS);
  ~Broker();

  void StartInNewThread();

  string AddChannel(const string& name);

  const std::shared_ptr<zmq::context_t>& GetContext() const;

  std::string GetEndpointByMachineId(const std::string& machine_id);

  std::string GetLocalMachineId() const;

private:
  string MakeEndpoint(const string& addr = "") const;

  /**
   * A broker only starts working after every other broker is up and sends a READY
   * message to everyone. There is one caveat: if after the synchronization happens, 
   * a machine goes down, and restarts, that machine cannot join anymore since the
   * READY messages are only sent once in the beginning.
   * In a real system, HEARTBEAT messages should be periodically sent out instead
   * to mitigate this problem.
   */
  bool InitializeConnection();
  
  void Run();

  void HandleIncomingMessage(MMessage&& msg);

  zmq::pollitem_t GetRouterPollItem();

  ConfigurationPtr config_;
  shared_ptr<zmq::context_t> context_;
  long poll_timeout_ms_;
  zmq::socket_t router_;

  // Thread stuff
  std::atomic<bool> running_;
  std::thread thread_;

  // Synchronization
  bool is_synchronized_;
  std::condition_variable cv_;
  std::mutex mutex_;

  // Messages that are sent to this broker when it is not READY yet
  vector<MMessage> unhandled_incoming_messages_;
  // Map from channel name to the channel
  unordered_map<string, zmq::socket_t> channels_;

  // Map from serialized-to-string MachineIds to IP addresses
  // Used to translate the identities of outgoing messages
  std::unordered_map<std::string, std::string> machine_id_to_endpoint_;

  // This is a hack so that tests behave correctly. Ideally, these sockets
  // should be scoped within InitializeConnection(). However, if we let them
  // linger indefinitely and a test ends before the cluster fully synchronized,
  // some of these sockets would hang up if their recipients already terminated.
  // If we don't let them linger at all, some of them might be destroyed at end
  // of function scope and the system would hang up, waiting for READY messages.
  // Putting them here solves the problem but is not ideal.
  std::vector<zmq::socket_t> tmp_sockets_;
};

} // namespace slog