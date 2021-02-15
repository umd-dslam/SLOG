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

using internal::Envelope;

NetworkedModule::NetworkedModule(const std::string& name, const std::shared_ptr<Broker>& broker, ChannelOption chopt,
                                 std::chrono::milliseconds poll_timeout, int recv_batch)
    : Module(name),
      context_(broker->context()),
      channel_(chopt.channel),
      pull_socket_(*context_, ZMQ_PULL),
      sender_(broker),
      poller_(poll_timeout),
      recv_batch_(recv_batch) {
  broker->AddChannel(channel_, chopt.recv_raw);
  pull_socket_.bind(MakeInProcChannelAddress(channel_));
  // Remove limit on the zmq message queues
  pull_socket_.set(zmq::sockopt::rcvhwm, 0);

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

zmq::socket_t& NetworkedModule::GetCustomSocket(size_t i) { return custom_sockets_[i]; }

const std::shared_ptr<zmq::context_t> NetworkedModule::context() const { return context_; }

void NetworkedModule::SetUp() {
  VLOG(1) << "Thread info: " << debug_info_;

  poller_.PushSocket(pull_socket_);

  Initialize();
}

bool NetworkedModule::Loop() {
  if (poller_.Wait() > 0) {
    for (int i = 0; i < recv_batch_; i++) {
      // Message from pull socket
      if (auto wrapped_env = RecvEnvelope(pull_socket_, true /* dont_wait */); wrapped_env != nullptr) {
#ifdef ENABLE_WORK_MEASURING
        auto start = std::chrono::steady_clock::now();
#endif

        EnvelopePtr env;
        if (wrapped_env->type_case() == Envelope::TypeCase::kRaw) {
          env.reset(new Envelope());
          if (!DeserializeProto(*env, wrapped_env->raw().data(), wrapped_env->raw().size())) {
            continue;
          }
          env->set_from(wrapped_env->from());
        } else {
          env = move(wrapped_env);
        }

        if (env->has_request()) {
          OnInternalRequestReceived(move(env));
        } else if (env->has_response()) {
          OnInternalResponseReceived(move(env));
        }

#ifdef ENABLE_WORK_MEASURING
        work_ += (std::chrono::steady_clock::now() - start).count();
#endif
      }

#ifdef ENABLE_WORK_MEASURING
      auto start = std::chrono::steady_clock::now();
      if (OnCustomSocket()) {
        work_ += (std::chrono::steady_clock::now() - start).count();
      }
#else
      OnCustomSocket();
#endif
    }
  }

#ifdef ENABLE_WORK_MEASURING
  auto start = std::chrono::steady_clock::now();
#endif

  OnWakeUp();

#ifdef ENABLE_WORK_MEASURING
  work_ += (std::chrono::steady_clock::now() - start).count();
#endif

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

}  // namespace slog