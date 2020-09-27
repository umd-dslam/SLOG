#include "connection/broker.h"

#include <sstream>
#include <unordered_set>

#include <glog/logging.h>

#include "common/mmessage.h"
#include "common/proto_utils.h"
#include "proto/internal.pb.h"

using std::pair;
using std::unordered_set;
using std::move;
using std::string;

namespace slog {

using internal::Request;

Broker::Broker(
    const ConfigurationPtr& config, 
    const shared_ptr<zmq::context_t>& context,
    long poll_timeout_ms) 
  : config_(config),
    context_(context),
    poll_timeout_ms_(poll_timeout_ms),
    router_(*context, ZMQ_ROUTER),
    running_(false),
    is_synchronized_(false) {
  // Set ZMQ_LINGER to 0 to discard all pending messages on shutdown.
  // Otherwise, it would hang indefinitely until the messages are sent.
  router_.setsockopt(ZMQ_LINGER, 0);
  // Remove all limits on the message queue
  router_.setsockopt(ZMQ_RCVHWM, 0);
}

Broker::~Broker() {
  running_ = false;
  thread_.join();
}

void Broker::StartInNewThread() {
  if (running_) {
    return;
  }
  running_ = true;
  thread_ = std::thread(&Broker::Run, this);
}

string Broker::AddChannel(const string& name) {
  CHECK(!running_) << "Cannot add new channel. The broker has already been running";
  CHECK(channels_.count(name) == 0) << "Channel \"" << name << "\" already exists";

  zmq::socket_t new_channel(*context_, ZMQ_PUSH);
  new_channel.setsockopt(ZMQ_LINGER, 0);
  new_channel.setsockopt(ZMQ_SNDHWM, 0);
  new_channel.connect("inproc://" + name);
  channels_[name] = move(new_channel);

  return name;
}

const std::shared_ptr<zmq::context_t>& Broker::GetContext() const {
  return context_;
}

std::string Broker::GetEndpointByMachineId(const std::string& machine_id) {
  std::unique_lock<std::mutex> lock(mutex_);
  while (!is_synchronized_) {
    cv_.wait(lock);
  }
  lock.unlock();

  // Once we can reach this point, we'll always be able to reach this point
  // and the machine_id_to_endpoint_ map becomes read-only.
  CHECK(machine_id_to_endpoint_.count(machine_id) > 0) << "Invalid machine id: " << machine_id;
  return machine_id_to_endpoint_.at(machine_id);
}

std::string Broker::GetLocalMachineId() const {
  return config_->GetLocalMachineIdAsString();
}

string Broker::MakeEndpoint(const string& addr) const {
  std::stringstream endpoint;
  const auto& protocol = config_->GetProtocol();
  endpoint << protocol << "://";
  if (addr.empty()) {
    if (protocol == "ipc") {
      endpoint << config_->GetLocalAddress();
    } else {
      endpoint << "*";
    }
  } else {
    endpoint << addr;
  }
  auto port = config_->GetBrokerPort();
  if (port > 0) {
    endpoint << ":" << port;
  }
  return endpoint.str();
}

bool Broker::InitializeConnection() {
  // Bind the router to its endpoint
  router_.bind(MakeEndpoint());
  LOG(INFO) << "Bound Broker to: " << MakeEndpoint();

  // Prepare a READY message
  Request request;
  auto ready = request.mutable_broker_ready();
  ready->set_ip_address(config_->GetLocalAddress());
  ready->mutable_machine_id()->CopyFrom(
      config_->GetLocalMachineIdAsProto());
  MMessage ready_msg;
  ready_msg.Set(MM_DATA, request);

  // Connect to all other machines and send the READY message
  auto local_machine_id = GetLocalMachineId();
  for (const auto& addr : config_->GetAllAddresses()) {
    zmq::socket_t socket(*context_, ZMQ_DEALER);
    socket.setsockopt(ZMQ_LINGER, 0);
    
    auto endpoint = MakeEndpoint(addr);
    socket.connect(endpoint);

    ready_msg.SendTo(socket);

    tmp_sockets_.push_back(move(socket));
    LOG(INFO) << "Sent READY message to " << endpoint;
  }

  // This set represents the membership of all machines in the network.
  // Each machine is identified with its replica and partition. Each broker
  // needs to receive the READY message from all other machines to start working.
  unordered_set<string> needed_machine_ids;
  for (uint32_t rep = 0; rep < config_->GetNumReplicas(); rep++) {
    for (uint32_t part = 0; part < config_->GetNumPartitions(); part++) {
      needed_machine_ids.insert(MakeMachineIdAsString(rep, part));
    }
  }

  LOG(INFO) << "Waiting for READY messages from other machines...";
  zmq::pollitem_t item = GetRouterPollItem();
  while (running_) {
    if (zmq::poll(&item, 1, poll_timeout_ms_)) {
      MMessage msg;
      msg.ReceiveFrom(router_);

      // The message must be a Request and it must be a READY request
      if (!msg.GetProto(request) || !request.has_broker_ready()) {
        LOG(INFO) << "Received a message while broker is not READY. "
                  << "Saving for later";
        unhandled_incoming_messages_.push_back(move(msg));
        continue;
      }

      // Use the information in each READY message to build up the translation maps
      const auto& ready = request.broker_ready();
      const auto& addr = ready.ip_address();
      const auto& machine_id = ready.machine_id();
      auto machine_id_str = MakeMachineIdAsString(
          machine_id.replica(), machine_id.partition());

      if (needed_machine_ids.count(machine_id_str) == 0) {
        continue;
      }

      LOG(INFO) << "Received READY message from " << addr 
                << " (rep: " << machine_id.replica() 
                << ", part: " << machine_id.partition() << ")";

      machine_id_to_endpoint_[machine_id_str] = MakeEndpoint(addr);
      needed_machine_ids.erase(machine_id_str);
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
  while (running_ && !unhandled_incoming_messages_.empty()) {
    HandleIncomingMessage(
        move(unhandled_incoming_messages_.back()));
    unhandled_incoming_messages_.pop_back();
  }

  // Set up poll items
  vector<zmq::pollitem_t> items;
  // NOTE: always push this item at the end 
  // so that the channel indices start from 0
  items.push_back(GetRouterPollItem());

  while (running_) {
    // Wait until a message arrived at one of the sockets
    zmq::poll(items, poll_timeout_ms_);

    // Router just received a message
    if (items.back().revents & ZMQ_POLLIN) {
      MMessage message(router_);
      HandleIncomingMessage(move(message));
    }

   VLOG_EVERY_N(4, 5000/poll_timeout_ms_) << "Broker is alive";
  } // while-loop
}

void Broker::HandleIncomingMessage(MMessage&& message) {
  // Remove the first byte of identity to get sender's machine id
  message.SetIdentity(message.GetIdentity().substr(1));
  CHECK(message.Size() >= 2) << "Insufficient information to broker to a channel";
  auto target_channel = message.Pop();
  if (channels_.count(target_channel) == 0) {
    LOG(ERROR) << "Unknown channel: \"" << target_channel << "\". Dropping message";
  } else {
    message.SendTo(channels_[target_channel]);
  }
}

zmq::pollitem_t Broker::GetRouterPollItem() {
  return {static_cast<void*>(router_), 0, ZMQ_POLLIN, 0};
}


} // namespace slog