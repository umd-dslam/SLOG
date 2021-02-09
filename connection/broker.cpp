#include "connection/broker.h"

#include <glog/logging.h>

#include <sstream>
#include <unordered_set>

#include "common/constants.h"
#include "common/proto_utils.h"
#include "common/thread_utils.h"
#include "proto/internal.pb.h"

using std::make_shared;
using std::move;
using std::pair;
using std::shared_ptr;
using std::string;
using std::unordered_set;

namespace slog {

using internal::Envelope;

shared_ptr<Broker> Broker::New(const ConfigurationPtr& config, std::chrono::milliseconds poll_timeout_ms, bool blocky) {
  auto context = make_shared<zmq::context_t>(1);
  context->set(zmq::ctxopt::blocky, blocky);
  return shared_ptr<Broker>(new Broker(config, context, poll_timeout_ms));
}

Broker::Broker(const ConfigurationPtr& config, const shared_ptr<zmq::context_t>& context,
               std::chrono::milliseconds poll_timeout_ms)
    : config_(config),
      context_(context),
      poll_timeout_ms_(poll_timeout_ms),
      external_socket_(*context, ZMQ_PULL),
      internal_socket_(*context, ZMQ_PULL),
      running_(false),
      is_synchronized_(false) {
  // Remove all limits on the message queue
  external_socket_.set(zmq::sockopt::rcvhwm, 0);
}

Broker::~Broker() {
  running_ = false;
  thread_.join();
#ifdef ENABLE_WORK_MEASURING
  LOG(INFO) << "Broker stopped. Work done: " << work_;
#endif
}

void Broker::StartInNewThread(std::optional<uint32_t> cpu) {
  if (running_) {
    return;
  }
  running_ = true;
  thread_ = std::thread(&Broker::Run, this);
  if (cpu.has_value()) {
    PinToCpu(pthread_self(), cpu.value());
  }
}

void Broker::Stop() { running_ = false; }

void Broker::AddChannel(Channel chan, bool send_raw) {
  CHECK(!running_) << "Cannot add new channel. The broker has already been running";
  CHECK(channels_.count(chan) == 0) << "Channel \"" << chan << "\" already exists";

  zmq::socket_t new_channel(*context_, ZMQ_PUSH);
  new_channel.set(zmq::sockopt::sndhwm, 0);
  new_channel.connect(MakeInProcChannelAddress(chan));
  channels_.try_emplace(chan, move(new_channel), send_raw);
}

std::string Broker::GetEndpointByMachineId(MachineId machine_id) {
  std::unique_lock<std::mutex> lock(mutex_);
  while (!is_synchronized_) {
    cv_.wait(lock);
  }
  lock.unlock();

  // Once we can reach this point, we'll always be able to reach this point
  // and the machine_id_to_endpoint_ map becomes read-only.
  auto endpoint_it = machine_id_to_endpoint_.find(machine_id);
  if (endpoint_it == machine_id_to_endpoint_.end()) {
    LOG(FATAL) << "Invalid machine id: " << machine_id;
  }
  return endpoint_it->second;
}

MachineId Broker::local_machine_id() const { return config_->local_machine_id(); }

string Broker::MakeEndpoint(const string& addr) const {
  std::stringstream endpoint;
  const auto& protocol = config_->protocol();
  endpoint << protocol << "://";
  if (addr.empty()) {
    if (protocol == "ipc") {
      endpoint << config_->local_address();
    } else {
      endpoint << "*";
    }
  } else {
    endpoint << addr;
  }
  auto port = config_->broker_port();
  if (port > 0) {
    endpoint << ":" << port;
  }
  return endpoint.str();
}

bool Broker::InitializeConnection() {
  // Bind the router to its endpoint
  external_socket_.bind(MakeEndpoint());
  LOG(INFO) << "Bound Broker to: " << MakeEndpoint();

  internal_socket_.bind(MakeInProcChannelAddress(kBrokerChannel));

  // Prepare a READY message
  Envelope env;
  auto ready = env.mutable_request()->mutable_broker_ready();
  ready->set_ip_address(config_->local_address());
  ready->set_machine_id(config_->local_machine_id());

  // Connect to all other machines and send the READY message
  for (const auto& addr : config_->all_addresses()) {
    zmq::socket_t tmp_socket(*context_, ZMQ_PUSH);

    auto endpoint = MakeEndpoint(addr);
    tmp_socket.connect(endpoint);

    SendSerializedProto(tmp_socket, env);

    // See comment in class declaration
    tmp_sockets_.push_back(move(tmp_socket));
    LOG(INFO) << "Sent READY message to " << endpoint;
  }

  // This set represents the membership of all machines in the network.
  // Each machine is identified with its replica and partition. Each broker
  // needs to receive the READY message from all other machines to start working.
  unordered_set<MachineId> needed_machine_ids;
  for (uint32_t rep = 0; rep < config_->num_replicas(); rep++) {
    for (uint32_t part = 0; part < config_->num_partitions(); part++) {
      needed_machine_ids.insert(config_->MakeMachineId(rep, part));
    }
  }

  LOG(INFO) << "Waiting for READY messages from other machines...";
  zmq::pollitem_t pollitem = {static_cast<void*>(external_socket_), 0, ZMQ_POLLIN, 0};
  while (running_) {
    if (zmq::poll(&pollitem, 1, poll_timeout_ms_)) {
      zmq::message_t msg;
      if (!external_socket_.recv(msg, zmq::recv_flags::dontwait)) {
        continue;
      }

      Envelope env;
      if (!DeserializeProto(env, msg) || !env.has_request() || !env.request().has_broker_ready()) {
        LOG(INFO) << "Received a message while broker is not READY. "
                  << "Saving for later";
        unhandled_incoming_messages_.push_back(move(msg));
        continue;
      }

      // Use the information in each READY message to build up the translation maps
      const auto& ready = env.request().broker_ready();
      const auto& addr = ready.ip_address();
      const auto machine_id = ready.machine_id();
      const auto [replica, partition] = config_->UnpackMachineId(machine_id);

      if (needed_machine_ids.count(machine_id) == 0) {
        continue;
      }

      LOG(INFO) << "Received READY message from " << addr << " (rep: " << replica << ", part: " << partition << ")";

      machine_id_to_endpoint_.insert_or_assign(machine_id, MakeEndpoint(addr));
      needed_machine_ids.erase(machine_id);
    }

    if (needed_machine_ids.empty()) {
      LOG(INFO) << "All READY messages received";
      return true;
    }
  }
  return false;
}

void Broker::Run() {
  if (!InitializeConnection()) {
    LOG(ERROR) << "Unable to initialize connection";
    return;
  }

  // Notify threads waiting in GetEndpointByMachineId() that all brokers
  // has been synchronized
  std::unique_lock<std::mutex> lock(mutex_);
  is_synchronized_ = true;
  lock.unlock();
  cv_.notify_all();

  // Handle the unhandled messages received during initializing
  for (size_t i = 0; i < unhandled_incoming_messages_.size(); i++) {
    HandleIncomingMessage(move(unhandled_incoming_messages_[i]));
  }
  unhandled_incoming_messages_.clear();

  vector<zmq::pollitem_t> pollitems = {{static_cast<void*>(external_socket_), 0, ZMQ_POLLIN, 0},
                                       {static_cast<void*>(internal_socket_), 0, ZMQ_POLLIN, 0}};
  while (running_) {
    // Wait until a message arrived at one of the sockets
    if (zmq::poll(pollitems, poll_timeout_ms_)) {
      for (int i = 0; i < 1000; i++) {
        if (zmq::message_t msg; external_socket_.recv(msg, zmq::recv_flags::dontwait)) {
#ifdef ENABLE_WORK_MEASURING
          auto start = std::chrono::steady_clock::now();
#endif

          HandleIncomingMessage(move(msg));

#ifdef ENABLE_WORK_MEASURING
          work_ += (std::chrono::steady_clock::now() - start).count();
#endif
        }
        if (auto env = RecvEnvelope(internal_socket_, true /* dont_wait */); env != nullptr) {
#ifdef ENABLE_WORK_MEASURING
          auto start = std::chrono::steady_clock::now();
#endif

          if (!env->has_request() || !env->request().has_broker_redirect()) {
            continue;
          }
          auto tag = env->request().broker_redirect().tag();
          if (env->request().broker_redirect().stop()) {
            redirect_.erase(tag);
            continue;
          }
          auto channel = env->request().broker_redirect().channel();
          auto chan_it = channels_.find(channel);
          if (chan_it == channels_.end()) {
            LOG(ERROR) << "Invalid channel to redirect to: \"" << channel << "\".";
            continue;
          }
          auto& entry = redirect_[tag];
          entry.to = channel;
          for (auto& msg : entry.pending_msgs) {
            ForwardMessage(chan_it->second.socket, chan_it->second.send_raw, move(msg));
          }
          entry.pending_msgs.clear();

#ifdef ENABLE_WORK_MEASURING
          work_ += (std::chrono::steady_clock::now() - start).count();
#endif
        }
      }
    }
  }  // while-loop
}

void Broker::HandleIncomingMessage(zmq::message_t&& msg) {
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

void Broker::ForwardMessage(zmq::socket_t& socket, bool send_raw, zmq::message_t&& msg) {
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

}  // namespace slog