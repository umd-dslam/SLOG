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
    running_(false) {
  // Set ZMQ_LINGER to 0 to discard all pending messages on shutdown.
  // Otherwise, it would hang indefinitely until the messages are sent.
  router_.setsockopt(ZMQ_LINGER, 0);
  // Remove all limits on the message queues
  router_.setsockopt(ZMQ_RCVHWM, 0);
  router_.setsockopt(ZMQ_SNDHWM, 0);
  for (const auto& addr : config->GetAllAddresses()) {
    auto socket = std::make_unique<zmq::socket_t>(*context, ZMQ_DEALER);
    socket->setsockopt(ZMQ_LINGER, 0);
    socket->setsockopt(ZMQ_RCVHWM, 0);
    socket->setsockopt(ZMQ_SNDHWM, 0);
    address_to_socket_[addr] = move(socket);
  }
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

unique_ptr<Channel> Broker::AddChannel(const string& name) {
  CHECK(!running_) << "Cannot add new channel. The broker has already been running";
  CHECK(channels_.count(name) == 0) << "Channel \"" << name << "\" already exists";

  channels_[name] = std::make_unique<Channel>(context_, name);
  return channels_[name]->GetListener();
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
  ready_msg.Set(MM_PROTO, request);

  // Connect to all other machines and send the READY message
  for (const auto& pair : address_to_socket_) {
    const auto& addr = pair.first;
    const auto& socket = pair.second;
    auto endpoint = MakeEndpoint(addr);
    socket->connect(endpoint);
    ready_msg.SendTo(*socket);
    LOG(INFO) << "Connected and sent READY message to " << endpoint;
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
      const auto& conn_id = msg.GetIdentity();
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

      machine_id_to_address_[machine_id_str] = addr;
      connection_id_to_machine_id_[conn_id] = machine_id_str;
      if (machine_id_str == config_->GetLocalMachineIdAsString()) {
        loopback_connection_id_ = conn_id;
      }

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
    return;
  }

  // Handle the unhandled messages received during initializing
  while (running_ && !unhandled_incoming_messages_.empty()) {
    HandleIncomingMessage(
        move(unhandled_incoming_messages_.back()));
    unhandled_incoming_messages_.pop_back();
  }

  // Set up poll items
  vector<zmq::pollitem_t> items;
  vector<string> channel_names;
  for (const auto& pair : channels_) {
    items.push_back(pair.second->GetPollItem());
    channel_names.push_back(pair.first);
  }
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

    for (size_t i = 0; i < items.size() - 1; i++) {
      // A channel just sent a message
      if (items[i].revents & ZMQ_POLLIN) {
        // Receive as many messages as it can based on the has_more
        // signal. This is done to balance out the message processing
        // speed. Some channel might produce messages faster than others.
        // When that happens, a fast channel has the option to chain
        // multiple messages and the broker will take them all out in one
        // go, reducing the queue size faster.
        bool has_more;
        do {
          MMessage message;
          has_more = channels_[channel_names[i]]->Receive(message);
          HandleOutgoingMessage(move(message));
        } while (has_more);
      }
    }

   VLOG_EVERY_N(4, 5000/poll_timeout_ms_) << "Broker is alive";
  } // while-loop
}

void Broker::HandleIncomingMessage(MMessage&& message) {
  // Translate connection id to slog id (replica, partition) 
  // before sending to the channel
  const auto& conn_id = message.GetIdentity();
  if (conn_id == loopback_connection_id_) {
    message.SetIdentity("");
  } else {
    if (connection_id_to_machine_id_.count(conn_id) == 0) {
      LOG(ERROR) << "Message came from an unknown connection (" << conn_id << "). Dropping";
      return;
    }
    message.SetIdentity(connection_id_to_machine_id_.at(conn_id));
  }
  SendToTargetChannel(move(message));
}

void Broker::HandleOutgoingMessage(MMessage&& message) {
  // If a message has an identity, it is sent out to the DEALER socket
  // corresponding to the identity. Otherwise, it is routed to another
  // channel on the same machine.
  if (message.HasIdentity()) {
    // Remove the identity part of the message before pop_backsending 
    // out to a DEALER socket
    const auto& machine_id = message.GetIdentity();
    CHECK(machine_id_to_address_.count(machine_id) > 0) << "Unknown machine id: " << machine_id;
    const auto& addr = machine_id_to_address_.at(machine_id);
    message.SetIdentity("");
    message.SendTo(*address_to_socket_.at(addr));
  } else {
    SendToTargetChannel(move(message));
  }
}

void Broker::SendToTargetChannel(MMessage&& msg) {
  CHECK(msg.Size() >= 2) << "Insufficient information to broker to a channel";
  auto target_channel = msg.Pop();
  if (channels_.count(target_channel) == 0) {
    LOG(ERROR) << "Unknown channel: \"" << target_channel << "\". Dropping message";
  } else {
    channels_[target_channel]->Send(msg);
  }
}

zmq::pollitem_t Broker::GetRouterPollItem() {
  return {static_cast<void*>(router_), 0, ZMQ_POLLIN, 0};
}


} // namespace slog