#include "module/base/networked_module.h"

#include <glog/logging.h>

#include <sstream>

#include "common/constants.h"
#include "connection/broker.h"
#include "connection/sender.h"

using std::make_unique;
using std::move;
using std::optional;
using std::unique_ptr;
using std::vector;

namespace slog {

using internal::Envelope;

NetworkedModule::NetworkedModule(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
                                 Channel channel, const MetricsRepositoryManagerPtr& metrics_manager,
                                 std::optional<std::chrono::milliseconds> poll_timeout)
    : context_(context),
      config_(config),
      channel_(channel),
      port_(std::nullopt),
      metrics_manager_(metrics_manager),
      inproc_socket_(*context_, ZMQ_PULL),
      sender_(config, context),
      poller_(poll_timeout),
      recv_retries_start_(config->recv_retries()),
      recv_retries_(0) {
  std::ostringstream os;
  os << "rep = " << config->local_replica() << ", part = " << config->local_partition()
     << ", machine_id = " << config->local_machine_id();
  debug_info_ = os.str();
}

NetworkedModule::NetworkedModule(const std::shared_ptr<Broker>& broker, ChannelOption chopt,
                                 const MetricsRepositoryManagerPtr& metrics_manager,
                                 optional<std::chrono::milliseconds> poll_timeout)
    : NetworkedModule(broker->context(), broker->config(), chopt.channel, metrics_manager, poll_timeout) {
  broker->AddChannel(channel_, chopt.recv_raw);
}

NetworkedModule::NetworkedModule(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
                                 uint32_t port, Channel channel, const MetricsRepositoryManagerPtr& metrics_manager,
                                 std::optional<std::chrono::milliseconds> poll_timeout)
    : NetworkedModule(context, config, channel, metrics_manager, poll_timeout) {
  port_ = port;
}

void NetworkedModule::AddCustomSocket(zmq::socket_t&& new_socket) {
  auto& sock = custom_sockets_.emplace_back(move(new_socket));
  poller_.PushSocket(sock);
}

zmq::socket_t& NetworkedModule::GetCustomSocket(size_t i) { return custom_sockets_.at(i); }

void NetworkedModule::SetUp() {
  VLOG(1) << "Thread info (" << name() << "): " << debug_info_;

  inproc_socket_.bind(MakeInProcChannelAddress(channel_));
  inproc_socket_.set(zmq::sockopt::rcvhwm, 0);
  poller_.PushSocket(inproc_socket_);

  if (port_.has_value()) {
    outproc_socket_ = zmq::socket_t(*context_, ZMQ_PULL);
    auto addr = MakeRemoteAddress(config_->protocol(), config_->local_address(), port_.value(), true /* binding */);
    outproc_socket_.bind(addr);
    outproc_socket_.set(zmq::sockopt::rcvhwm, 0);

    LOG(INFO) << "Bound " << name() << " to \"" << addr << "\"";

    poller_.PushSocket(outproc_socket_);
  }

  if (metrics_manager_ != nullptr) {
    metrics_manager_->RegisterCurrentThread();
  }

  Initialize();
}

bool NetworkedModule::Loop() {
  if (!poller_.NextEvent(recv_retries_ > 0 /* dont_wait */)) {
    return false;
  }

  if (OnEnvelopeReceived(RecvEnvelope(inproc_socket_, true /* dont_wait */))) {
    recv_retries_ = recv_retries_start_;
  }

  if (outproc_socket_.handle() != ZMQ_NULLPTR) {
    if (zmq::message_t msg; outproc_socket_.recv(msg, zmq::recv_flags::dontwait)) {
      auto env = DeserializeEnvelope(msg);
      if (OnEnvelopeReceived(move(env))) {
        recv_retries_ = recv_retries_start_;
      }
    }
  }

  if (OnCustomSocket()) {
    recv_retries_ = recv_retries_start_;
  }

  if (recv_retries_ > 0) {
    recv_retries_--;
  }

  return false;
}

bool NetworkedModule::OnEnvelopeReceived(EnvelopePtr&& wrapped_env) {
  if (wrapped_env == nullptr) {
    return false;
  }
  EnvelopePtr env;
  if (wrapped_env->type_case() == Envelope::TypeCase::kRaw) {
    env.reset(new Envelope());
    if (DeserializeProto(*env, wrapped_env->raw().data(), wrapped_env->raw().size())) {
      env->set_from(wrapped_env->from());
    }
  } else {
    env = move(wrapped_env);
  }

  if (env->has_request()) {
    if (env->request().has_ping()) {
      Envelope pong_env;
      auto pong = pong_env.mutable_response()->mutable_pong();
      pong->set_time(env->request().ping().time());
      pong->set_target(env->request().ping().target());
      Send(move(pong_env), env->from(), env->request().ping().from_channel());
    } else {
      OnInternalRequestReceived(move(env));
    }
  } else if (env->has_response()) {
    OnInternalResponseReceived(move(env));
  }

  return true;
}

void NetworkedModule::Send(const Envelope& env, MachineId to_machine_id, Channel to_channel) {
  sender_.Send(env, to_machine_id, to_channel);
}

void NetworkedModule::Send(EnvelopePtr&& env, MachineId to_machine_id, Channel to_channel) {
  sender_.Send(move(env), to_machine_id, to_channel);
}

void NetworkedModule::Send(EnvelopePtr&& env, Channel to_channel) { sender_.Send(move(env), to_channel); }

void NetworkedModule::Send(const Envelope& env, const std::vector<MachineId>& to_machine_ids, Channel to_channel) {
  sender_.Send(env, to_machine_ids, to_channel);
}

void NetworkedModule::Send(EnvelopePtr&& env, const std::vector<MachineId>& to_machine_ids, Channel to_channel) {
  sender_.Send(move(env), to_machine_ids, to_channel);
}

void NetworkedModule::NewTimedCallback(std::chrono::microseconds timeout, std::function<void()>&& cb) {
  poller_.AddTimedCallback(timeout, std::move(cb));
}

}  // namespace slog