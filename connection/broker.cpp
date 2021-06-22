#include "connection/broker.h"

#include <glog/logging.h>

#include "common/constants.h"
#include "common/proto_utils.h"
#include "common/thread_utils.h"
#include "connection/zmq_utils.h"
#include "proto/internal.pb.h"

using std::make_shared;
using std::move;
using std::pair;
using std::shared_ptr;
using std::string;
using std::unordered_map;
using std::vector;

namespace slog {

using internal::Envelope;

namespace {
class BrokerThread : public Module {
 public:
  BrokerThread(const shared_ptr<zmq::context_t>& context, const string& internal_endpoint,
               const string& external_endpoint, const vector<pair<Channel, bool>>& channels, int recv_retries_start_,
               std::chrono::milliseconds poll_timeout_ms)
      : external_socket_(*context, ZMQ_PULL),
        internal_socket_(*context, ZMQ_PULL),
        internal_endpoint_(internal_endpoint),
        external_endpoint_(external_endpoint),
        poll_timeout_ms_(poll_timeout_ms),
        recv_retries_start_(recv_retries_start_),
        recv_retries_(0) {
    // Remove all limits on the message queue
    external_socket_.set(zmq::sockopt::rcvhwm, 0);

    for (auto [chan, send_raw] : channels) {
      DCHECK(channels_.find(chan) == channels_.end()) << "Duplicate channel: " << chan;
      zmq::socket_t new_channel(*context, ZMQ_PUSH);
      new_channel.set(zmq::sockopt::sndhwm, 0);
      new_channel.connect(MakeInProcChannelAddress(chan));
      channels_.try_emplace(chan, move(new_channel), send_raw);
    }
  }

  std::string name() const override { return "Broker"; };

  void SetUp() final {
    LOG(INFO) << "Binding a broker thread to \"" << external_endpoint_ << "\"";

    external_socket_.bind(external_endpoint_);
    internal_socket_.bind(internal_endpoint_);

    poll_items_ = {{static_cast<void*>(external_socket_), 0, ZMQ_POLLIN, 0},
                   {static_cast<void*>(internal_socket_), 0, ZMQ_POLLIN, 0}};
  }

  bool Loop() final {
    if (recv_retries_ <= 0 && !zmq::poll(poll_items_, poll_timeout_ms_)) {
      return false;
    }

    if (zmq::message_t msg; external_socket_.recv(msg, zmq::recv_flags::dontwait)) {
      recv_retries_ = recv_retries_start_;
      HandleIncomingMessage(move(msg));
    }

    if (auto env = RecvEnvelope(internal_socket_, true /* dont_wait */); env != nullptr) {
      recv_retries_ = recv_retries_start_;

      if (!env->has_request() || !env->request().has_broker_redirect()) {
        return false;
      }
      auto tag = env->request().broker_redirect().tag();
      if (env->request().broker_redirect().stop()) {
        redirect_.erase(tag);
        return false;
      }
      auto channel = env->request().broker_redirect().channel();
      auto chan_it = channels_.find(channel);
      if (chan_it == channels_.end()) {
        LOG(ERROR) << "Invalid channel to redirect to: \"" << channel << "\".";
        return false;
      }
      auto& entry = redirect_[tag];
      entry.to = channel;
      for (auto& msg : entry.pending_msgs) {
        ForwardMessage(chan_it->second.socket, chan_it->second.send_raw, move(msg));
      }
      entry.pending_msgs.clear();
    }

    if (recv_retries_ > 0) {
      --recv_retries_;
    }

    return false;
  }

 private:
  void HandleIncomingMessage(zmq::message_t&& msg) {
    Channel tag_or_chan_id;
    if (!ParseChannel(tag_or_chan_id, msg)) {
      LOG(ERROR) << "Message without channel info";
      return;
    }

    auto chan_id = tag_or_chan_id;

    // Check if this is a tag or a channel id.
    // This condition effectively allows sending messages to a worker only via redirection since
    // workers' channel ids are larger than kMaxChannel
    if (tag_or_chan_id >= kMaxChannel) {
      auto& entry = redirect_[tag_or_chan_id];
      if (!entry.to.has_value()) {
        entry.pending_msgs.push_back(move(msg));
        return;
      }
      chan_id = entry.to.value();
    }

    auto chan_it = channels_.find(chan_id);
    if (chan_it == channels_.end()) {
      LOG(ERROR) << "Unknown channel: \"" << chan_id << "\". Dropping message";
      return;
    }
    ForwardMessage(chan_it->second.socket, chan_it->second.send_raw, move(msg));
  }

  void ForwardMessage(zmq::socket_t& socket, bool send_raw, zmq::message_t&& msg) {
    MachineId machine_id = -1;
    ParseMachineId(machine_id, msg);

    auto env = std::make_unique<Envelope>();
    if (send_raw) {
      env->set_raw(msg.to_string());
    } else {
      if (!DeserializeProto(*env, msg)) {
        LOG(ERROR) << "Malformed message";
        return;
      }
    }

    // This must be set AFTER deserializing otherwise it will be overwritten by the deserialization function
    env->set_from(machine_id);
    SendEnvelope(socket, move(env));
  }

  zmq::socket_t external_socket_;
  zmq::socket_t internal_socket_;
  const string internal_endpoint_;
  const string external_endpoint_;
  std::chrono::milliseconds poll_timeout_ms_;
  vector<zmq::pollitem_t> poll_items_;
  int recv_retries_start_;
  int recv_retries_;

  struct ChannelEntry {
    ChannelEntry(zmq::socket_t&& socket, bool send_raw) : socket(std::move(socket)), send_raw(send_raw) {}
    zmq::socket_t socket;
    const bool send_raw;
  };
  unordered_map<Channel, ChannelEntry> channels_;

  struct RedirectEntry {
    std::optional<Channel> to;
    vector<zmq::message_t> pending_msgs;
  };
  unordered_map<uint64_t, RedirectEntry> redirect_;
};
}  // namespace

shared_ptr<Broker> Broker::New(const ConfigurationPtr& config, std::chrono::milliseconds poll_timeout_ms, bool blocky) {
  auto context = make_shared<zmq::context_t>(1);
  context->set(zmq::ctxopt::blocky, blocky);
  return shared_ptr<Broker>(new Broker(config, context, poll_timeout_ms));
}

Broker::Broker(const ConfigurationPtr& config, const shared_ptr<zmq::context_t>& context,
               std::chrono::milliseconds poll_timeout_ms)
    : config_(config), context_(context), poll_timeout_ms_(poll_timeout_ms), running_(false) {}

void Broker::AddChannel(Channel chan, bool send_raw) {
  CHECK(!running_) << "Cannot add new channel. The broker has already been running";
  channels_.emplace_back(chan, send_raw);
}

void Broker::StartInNewThreads() {
  DCHECK(!running_) << "Broker is already running";
  if (running_) {
    return;
  }
  running_ = true;

  auto cpus = config_->cpu_pinnings(ModuleId::BROKER);
  for (size_t i = 0; i < config_->broker_ports_size(); i++) {
    auto internal_endpoint = MakeInProcChannelAddress(MakeChannel(i));
    auto external_endpoint =
        MakeRemoteAddress(config_->protocol(), config_->local_address(), config_->broker_ports(i), true /* binding */);

    auto& t = threads_.emplace_back(MakeRunnerFor<BrokerThread>(context_, internal_endpoint, external_endpoint,
                                                                channels_, config_->recv_retries(), poll_timeout_ms_));

    std::optional<uint32_t> cpu = {};
    if (i < cpus.size()) {
      cpu = cpus[i];
    }

    t->StartInNewThread(cpu);
  }
}

void Broker::Stop() {
  for (auto& t : threads_) {
    t->Stop();
  }
  running_ = false;
}

}  // namespace slog