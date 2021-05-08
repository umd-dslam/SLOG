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

NetworkedModule::NetworkedModule(const std::string& name, const std::shared_ptr<Broker>& broker, ChannelOption chopt,
                                 const MetricsRepositoryManagerPtr& metrics_manager,
                                 optional<std::chrono::milliseconds> poll_timeout)
    : Module(name),
      context_(broker->context()),
      config_(broker->config()),
      metrics_manager_(metrics_manager),
      channel_(chopt.channel),
      pull_socket_(*context_, ZMQ_PULL),
      sender_(broker->config(), broker->context()),
      poller_(poll_timeout),
      recv_retries_start_(broker->config()->recv_retries()),
      recv_retries_(0),
      weights_({1, 1}),
      counters_({0, 0}),
      current_(0) {
  broker->AddChannel(channel_, chopt.recv_raw);
  poller_.PushSocket(pull_socket_);

  auto& config = broker->config();
  std::ostringstream os;
  os << "module = " << name << ", rep = " << config->local_replica() << ", part = " << config->local_partition()
     << ", machine_id = " << config->local_machine_id();
  debug_info_ = os.str();
}

NetworkedModule::~NetworkedModule() {
#ifdef ENABLE_WORK_MEASURING
  LOG(INFO) << name() << " - work done: " << work_;
#endif
}

void NetworkedModule::AddCustomSocket(zmq::socket_t&& new_socket) {
  auto& sock = custom_sockets_.emplace_back(move(new_socket));
  poller_.PushSocket(sock);
}

zmq::socket_t& NetworkedModule::GetCustomSocket(size_t i) { return custom_sockets_.at(i); }

void NetworkedModule::SetUp() {
  VLOG(1) << "Thread info: " << debug_info_;

  pull_socket_.bind(MakeInProcChannelAddress(channel_));
  // Remove limit on the zmq message queues
  pull_socket_.set(zmq::sockopt::rcvhwm, 0);

  if (metrics_manager_ != nullptr) {
    metrics_manager_->RegisterCurrentThread();
  }

  Initialize();
}

bool NetworkedModule::Loop() {
  if (!poller_.NextEvent(recv_retries_ > 0 /* dont_wait */)) {
    return false;
  }

  bool got_message = false;
  if (current_ == 0) {
    auto wrapped_env = RecvEnvelope(pull_socket_, true /* dont_wait */);
    if (wrapped_env != nullptr) {
      got_message = true;
      recv_retries_ = recv_retries_start_;

#ifdef ENABLE_WORK_MEASURING
      auto start = std::chrono::steady_clock::now();
#endif

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

#ifdef ENABLE_WORK_MEASURING
      work_ += (std::chrono::steady_clock::now() - start).count();
#endif
    }
  }

  if (current_ == 1) {
#ifdef ENABLE_WORK_MEASURING
    auto start = std::chrono::steady_clock::now();
#endif
    if (OnCustomSocket()) {
      got_message = true;
      recv_retries_ = recv_retries_start_;

#ifdef ENABLE_WORK_MEASURING
      work_ += (std::chrono::steady_clock::now() - start).count();
#endif
    }
  }

  if (recv_retries_ > 0) {
    recv_retries_--;
  }

  if (got_message) {
    counters_[current_]++;
  }
  if (!got_message || counters_[current_] >= weights_[current_]) {
    // If there is no custom socket, we don't need to switch to the custom socket weight
    uint8_t has_custom_sockets = !custom_sockets_.empty();
    current_ = (current_ + 1) & has_custom_sockets;
    counters_[current_] = 0;
  }

  return false;
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

void NetworkedModule::NewTimedCallback(microseconds timeout, std::function<void()>&& cb) {
  poller_.AddTimedCallback(timeout, std::move(cb));
}

void NetworkedModule::ClearTimedCallbacks() { poller_.ClearTimedCallbacks(); }

}  // namespace slog