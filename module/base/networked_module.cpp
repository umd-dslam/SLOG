#include "module/base/networked_module.h"

#include <glog/logging.h>

#include <sstream>

#include "common/constants.h"
#include "connection/broker.h"
#include "connection/sender.h"

using std::make_unique;
using std::move;
using std::unique_ptr;
using std::vector;

namespace slog {

NetworkedModule::NetworkedModule(const std::string& name, const std::shared_ptr<Broker>& broker, Channel channel,
                                 std::chrono::milliseconds poll_timeout, int recv_batch)
    : Module(name),
      context_(broker->context()),
      channel_(channel),
      pull_socket_(*context_, ZMQ_PULL),
      sender_(broker),
      poller_(poll_timeout),
      recv_batch_(recv_batch) {
  broker->AddChannel(channel);
  pull_socket_.bind("inproc://channel_" + std::to_string(channel));
  // Remove limit on the zmq message queues
  pull_socket_.set(zmq::sockopt::rcvhwm, 0);

  auto& config = broker->config();
  std::ostringstream os;
  os << "module = " << name << ", rep = " << config->local_replica() << ", part = " << config->local_partition()
     << ", machine_id = " << config->local_machine_id();
  debug_info_ = os.str();
}

zmq::socket_t& NetworkedModule::GetCustomSocket(size_t i) { return custom_sockets_[i]; }

const std::shared_ptr<zmq::context_t> NetworkedModule::context() const { return context_; }

void NetworkedModule::SetUp() {
  VLOG(1) << "Thread info: " << debug_info_;

  poller_.PushSocket(pull_socket_);
  custom_sockets_ = InitializeCustomSockets();
  for (auto& socket : custom_sockets_) {
    poller_.PushSocket(socket);
  }

  Initialize();
}

bool NetworkedModule::Loop() {
  auto poll_res = poller_.Wait();

  for (auto time_ev : poll_res.time_events) {
    HandleTimeEvent(time_ev);
  }

  if (poll_res.num_zmq_events == 0) {
    return false;
  }

  for (int i = 0; i < recv_batch_; i++) {
    // Message from pull socket
    if (auto env = RecvEnvelope(pull_socket_, true /* dont_wait */); env != nullptr) {
      if (env->has_request()) {
        HandleInternalRequest(move(env));
      } else if (env->has_response()) {
        HandleInternalResponse(move(env));
      }
    }

    for (size_t i = 0; i < custom_sockets_.size(); i++) {
      HandleCustomSocket(custom_sockets_[i], i);
    }
  }

  return false;
}

void NetworkedModule::Send(const internal::Envelope& env, MachineId to_machine_id, Channel to_channel) {
  sender_.SendSerialized(env, to_machine_id, to_channel);
}

void NetworkedModule::Send(EnvelopePtr&& env, Channel to_channel) { sender_.SendLocal(move(env), to_channel); }

void NetworkedModule::NewTimeEvent(microseconds timeout, void* data) { poller_.AddTimeEvent(timeout, data); }

}  // namespace slog