#pragma once

#include <vector>
#include <zmq.hpp>

#include "common/constants.h"
#include "common/message_pool.h"
#include "common/types.h"
#include "connection/broker.h"
#include "connection/poller.h"
#include "connection/sender.h"
#include "module/base/module.h"
#include "proto/internal.pb.h"

namespace slog {

using ReusableRequest = ReusableMessage<internal::Request>;
using ReusableResponse = ReusableMessage<internal::Response>;

/**
 * Base class for modules that can send and receive in internal messages.
 */
class NetworkedModule : public Module {
 public:
  NetworkedModule(const std::string& name, const std::shared_ptr<Broker>& broker, Channel channel,
                  std::chrono::milliseconds poll_timeout, int recv_batch = 5000, size_t request_pool_size = 5000,
                  size_t response_pool_size = 5000);

 protected:
  virtual std::vector<zmq::socket_t> InitializeCustomSockets() { return {}; }

  virtual void Initialize(){};

  virtual void HandleInternalRequest(ReusableRequest&& req, MachineId from) = 0;

  virtual void HandleInternalResponse(ReusableResponse&& /* res */, MachineId /* from */) {}

  virtual void HandleTimeEvent(void* /* data */) {}

  // The implementation of this function must never block
  virtual void HandleCustomSocket(zmq::socket_t& /* socket */, size_t /* socket_index */){};

  zmq::socket_t& GetCustomSocket(size_t i);

  ReusableRequest NewRequest() {
    ReusableMessage msg{&request_pool_};
    msg.get()->Clear();
    return msg;
  }

  ReusableResponse NewResponse() {
    ReusableMessage msg{&response_pool_};
    msg.get()->Clear();
    return msg;
  }

  void Send(const google::protobuf::Message& request_or_response, Channel to_channel, MachineId to_machine_id);

  void Send(const google::protobuf::Message& request_or_response, Channel to_channel);

  void NewTimeEvent(microseconds timeout, void* data);

  const std::shared_ptr<zmq::context_t> context() const;

  Channel channel() const { return channel_; }

 private:
  void SetUp() final;
  bool Loop() final;

  std::shared_ptr<zmq::context_t> context_;
  Channel channel_;
  zmq::socket_t pull_socket_;
  std::vector<zmq::socket_t> custom_sockets_;
  Sender sender_;
  Poller poller_;
  int recv_batch_;
  MessagePool<internal::Request> request_pool_;
  MessagePool<internal::Response> response_pool_;
  std::string debug_info_;
};

}  // namespace slog