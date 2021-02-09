#pragma once

#include <condition_variable>
#include <thread>
#include <unordered_map>
#include <zmq.hpp>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/types.h"
#include "connection/zmq_utils.h"

namespace slog {

/**
 * A Broker distributes messages coming into a machine to the modules
 * It runs its own thread with the components depicted below
 *
 *                   --------------------------
 *                   |                        |
 *  Module A <---- Channel A                Router  <----- Incoming Messages
 *                   |          B             |
 *                   |           R            |
 *  Module B <---- Channel B      O           |
 *                   |             K          |
 *                   |              E         |
 *  Module C <---- Channel C         R        |
 *                   |                        |
 *                   |                        |
 *                   --------------------------
 *                      ^         ^         ^
 *                      |         |         |
 *                    < Broker Synchronization >
 *                      |         |         |
 *                      |         |         |
 *  Module A  ------> Sender -----------------------> Outgoing Messages
 *                                |         |
 *  Module B  ----------------> Sender -------------> Outgoing Messages
 *                                          |
 *  Module C  --------------------------> Sender ---> Outgoing Messages
 *
 *
 * To receive messages from other machines, it uses a ZMQ_ROUTER socket, which constructs
 * a map from an identity to the corresponding connection. Using this identity, it can
 * tell where the message comes from.
 *
 * The messages going into the system via the router will be brokered to the channel
 * specified in each message. On the other end of each channel is a module which also runs
 * in its own thread.
 *
 * A module sends message to another machine via a Sender object. Each Sender object maintains
 * a weak pointer to the broker to get notified when the brokers are synchronized and to access
 * the map translating logical machine IDs to physical machine addresses.
 *
 * Not showed above: the modules can send message to each other using Sender without going through the Broker.
 */
class Broker {
 public:
  static std::shared_ptr<Broker> New(const ConfigurationPtr& config,
                                     std::chrono::milliseconds poll_timeout_ms = kModuleTimeout, bool blocky = false);

  ~Broker();

  void StartInNewThread(std::optional<uint32_t> cpu = {});

  void Stop();

  void AddChannel(Channel chan, bool send_raw = false);

  const ConfigurationPtr& config() const { return config_; }
  const std::shared_ptr<zmq::context_t>& context() const { return context_; }

  std::string GetEndpointByMachineId(MachineId machine_id);

  MachineId local_machine_id() const;

 private:
  Broker(const ConfigurationPtr& config, const std::shared_ptr<zmq::context_t>& context,
         std::chrono::milliseconds poll_timeout_ms);

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

  void HandleIncomingMessage(zmq::message_t&& msg);

  void ForwardMessage(zmq::socket_t& socket, bool send_raw, zmq::message_t&& msg);

  ConfigurationPtr config_;
  std::shared_ptr<zmq::context_t> context_;
  std::chrono::milliseconds poll_timeout_ms_;
  zmq::socket_t external_socket_;
  zmq::socket_t internal_socket_;

  // Thread stuff
  std::atomic<bool> running_;
  std::thread thread_;

  // For synchronizing with the senders
  bool is_synchronized_;
  std::condition_variable cv_;
  std::mutex mutex_;

  // Messages that are sent to this broker when it is not READY yet
  vector<zmq::message_t> unhandled_incoming_messages_;

  struct ChannelEntry {
    ChannelEntry(zmq::socket_t&& socket, bool send_raw) : socket(std::move(socket)), send_raw(send_raw) {}
    zmq::socket_t socket;
    const bool send_raw;
  };
  std::unordered_map<Channel, ChannelEntry> channels_;

  struct RedirectEntry {
    std::optional<Channel> to;
    std::vector<zmq::message_t> pending_msgs;
  };
  std::unordered_map<Channel, RedirectEntry> redirect_;

  // Map from serialized-to-string MachineIds to IP addresses
  // Used to translate the identities of outgoing messages
  std::unordered_map<MachineId, std::string> machine_id_to_endpoint_;

  // This is a hack so that tests behave correctly. Ideally, these sockets
  // should be scoped within InitializeConnection(). However, if we let them
  // linger indefinitely and a test ends before the cluster is fully synchronized,
  // some of these sockets would hang up if one of their READY message recipients
  // is already terminated, leaving the READY message unconsumed in the queue and
  // in turn hanging up the cleaning up process of the test. If we don't let them
  // linger at all, some of them  might be destroyed at end of function scope and
  // the READY message does not have enought time to be sent out.
  // Putting them here solves those problems but is not ideal.
  std::vector<zmq::socket_t> tmp_sockets_;

  std::atomic<uint64_t> work_ = 0;
};

}  // namespace slog