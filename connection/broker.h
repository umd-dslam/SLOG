#pragma once

#include <condition_variable>
#include <thread>
#include <unordered_map>
#include <zmq.hpp>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/types.h"
#include "connection/zmq_utils.h"
#include "module/base/module.h"

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
 *
 *  Module A  ------> Sender -----------------------> Outgoing Messages
 *
 *  Module B  ----------------> Sender -------------> Outgoing Messages
 *
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
 * A module sends message to another machine via a Sender object. Not showed above: the modules
 * can send message to each other using Sender without going through the Broker.
 */
class Broker {
 public:
  static std::shared_ptr<Broker> New(const ConfigurationPtr& config,
                                     std::chrono::milliseconds poll_timeout_ms = kModuleTimeout, bool blocky = false);

  static Channel MakeChannel(int broker_num) { return kBrokerChannel + broker_num; }

  void StartInNewThreads();
  void Stop();

  void AddChannel(Channel chan, bool send_raw = false);

  const ConfigurationPtr& config() const { return config_; }
  const std::shared_ptr<zmq::context_t>& context() const { return context_; }

 private:
  Broker(const ConfigurationPtr& config, const std::shared_ptr<zmq::context_t>& context,
         std::chrono::milliseconds poll_timeout_ms);

  ConfigurationPtr config_;
  std::shared_ptr<zmq::context_t> context_;
  std::chrono::milliseconds poll_timeout_ms_;

  bool running_;
  std::vector<std::pair<Channel, bool>> channels_;
  std::vector<std::unique_ptr<slog::ModuleRunner>> threads_;
};

}  // namespace slog