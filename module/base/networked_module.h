#pragma once

#include <vector>
#include <zmq.hpp>

#include "common/constants.h"
#include "common/metrics.h"
#include "common/types.h"
#include "connection/broker.h"
#include "connection/poller.h"
#include "connection/sender.h"
#include "connection/zmq_utils.h"
#include "module/base/module.h"
#include "proto/internal.pb.h"

namespace slog {

struct ChannelOption {
  ChannelOption(Channel channel, bool recv_raw = true) : channel(channel), recv_raw(recv_raw) {}
  Channel channel;
  bool recv_raw;
};

/**
 * Base class for modules that can send and receive in internal messages.
 */
class NetworkedModule : public Module {
 public:
  NetworkedModule(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config, Channel channel,
                  const MetricsRepositoryManagerPtr& metrics_manager,
                  std::optional<std::chrono::milliseconds> poll_timeout);

  NetworkedModule(const std::shared_ptr<Broker>& broker, ChannelOption chopt,
                  const MetricsRepositoryManagerPtr& metrics_manager,
                  std::optional<std::chrono::milliseconds> poll_timeout);

  NetworkedModule(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config, uint32_t port,
                  Channel channel, const MetricsRepositoryManagerPtr& metrics_manager,
                  std::optional<std::chrono::milliseconds> poll_timeout);

 protected:
  virtual void Initialize(){};

  virtual void OnInternalRequestReceived(EnvelopePtr&& env) = 0;

  virtual void OnInternalResponseReceived(EnvelopePtr&& /* env */) {}

  // Returns true if useful work was done
  virtual bool OnCustomSocket() { return false; }

  void AddCustomSocket(zmq::socket_t&& new_socket);
  zmq::socket_t& GetCustomSocket(size_t i);

  inline static EnvelopePtr NewEnvelope() { return std::make_unique<internal::Envelope>(); }
  void Send(const internal::Envelope& env, MachineId to_machine_id, Channel to_channel);
  void Send(EnvelopePtr&& env, MachineId to_machine_id, Channel to_channel);
  void Send(EnvelopePtr&& env, Channel to_channel);
  void Send(const internal::Envelope& env, const std::vector<MachineId>& to_machine_ids, Channel to_channel);
  void Send(EnvelopePtr&& env, const std::vector<MachineId>& to_machine_ids, Channel to_channel);

  void NewTimedCallback(std::chrono::microseconds timeout, std::function<void()>&& cb);

  const std::shared_ptr<zmq::context_t>& context() const { return context_; }
  const ConfigurationPtr& config() const { return config_; }

  Channel channel() const { return channel_; }
  MetricsRepositoryManager& metrics_manager() { return *metrics_manager_; }

 private:
  void SetUp() final;
  bool Loop() final;

  bool OnEnvelopeReceived(EnvelopePtr&& wrapped_env);

  std::shared_ptr<zmq::context_t> context_;
  ConfigurationPtr config_;
  Channel channel_;
  std::optional<uint32_t> port_;
  MetricsRepositoryManagerPtr metrics_manager_;
  zmq::socket_t inproc_socket_;
  zmq::socket_t outproc_socket_;
  std::vector<zmq::socket_t> custom_sockets_;
  Sender sender_;
  Poller poller_;
  int recv_retries_start_;
  int recv_retries_;

  std::string debug_info_;

  std::atomic<uint64_t> work_ = 0;
};

}  // namespace slog