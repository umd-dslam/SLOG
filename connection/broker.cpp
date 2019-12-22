#include "connection/broker.h"

#include <sstream>
#include <unordered_set>

#include <glog/logging.h>

#include "common/mmessage.h"
#include "common/proto_utils.h"
#include "proto/request.pb.h"

using std::pair;
using std::unordered_set;
namespace slog {

namespace {
const long BROKER_POLL_TIMEOUT_MS = 1000;
}

const std::string Broker::SERVER_CHANNEL = "server";
const std::string Broker::SEQUENCER_CHANNEL = "sequencer";
const std::string Broker::SCHEDULER_CHANNEL = "scheduler";

Broker::Broker(
    shared_ptr<Configuration> config, 
    shared_ptr<zmq::context_t> context) 
  : config_(config),
    context_(context),
    router_(*context, ZMQ_ROUTER),
    running_(false) {
  // Set ZMQ_LINGER to 0 to discard all pending messages on shutdown.
  // Otherwise, it would hang indefinitely until the messages are sent.
  router_.setsockopt(ZMQ_LINGER, 0);
  for (const auto& addr : config->GetAllAddresses()) {
    auto socket = std::make_unique<zmq::socket_t>(*context, ZMQ_DEALER);
    socket->setsockopt(ZMQ_LINGER, 0);
    address_to_socket_[addr] = std::move(socket);
  }
}

Broker::~Broker() {
  running_ = false;
  thread_.join();
}

void Broker::Start() {
  running_ = true;
  thread_ = std::thread(&Broker::Run, this);
}

ChannelListener* Broker::AddChannel(const std::string& name) {
  CHECK(!running_) << "Cannot add new channel. The broker has already been running";
  CHECK(channels_.count(name) == 0) << "Channel \"" << name << "\" already exists";

  channels_[name] = std::make_unique<Channel>(context_, SERVER_CHANNEL);
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
  LOG(INFO) << "Bound broker to: " << MakeEndpoint();

  // Prepare a READY message
  proto::Request request;
  auto ready = request.mutable_ready_req();
  ready->set_ip_address(config_->GetLocalAddress());
  *ready->mutable_slog_id() = config_->GetLocalSlogId();
  MMessage ready_msg(request);

  // Connect to all other machines and send the READY message
  for (const auto& pair : address_to_socket_) {
    const auto& addr = pair.first;
    const auto& socket = pair.second;
    auto endpoint = MakeEndpoint(addr);
    socket->connect(endpoint);
    ready_msg.Send(*socket);
    LOG(INFO) << "Connected and sent READY message to " << endpoint;
  }

  // This set represents the membership of all machines in the network.
  // Each machine is identified with its replica and partition. Each broker
  // needs to receive the READY message from all other machines to start working.
  unordered_set<string> needed_slog_ids;
  for (uint32_t rep = 0; rep < config_->GetNumReplicas(); rep++) {
    for (uint32_t part = 0; part < config_->GetNumPartitions(); part++) {
      auto slog_id = MakeSlogId(rep, part);
      needed_slog_ids.insert(SlogIdToString(slog_id));
    }
  }

  LOG(INFO) << "Waiting for READY messages from other machines...";
  zmq::pollitem_t item = { static_cast<void*>(router_), 0, ZMQ_POLLIN, 0 };
  while (running_) {
    zmq::poll(&item, 1, BROKER_POLL_TIMEOUT_MS);
    if (item.revents & ZMQ_POLLIN) {
      ready_msg.Receive(router_);
      
      // The message must be a Request
      if (!ready_msg.ToRequest(request)) {
        continue;
      }
      
      // The request message must be READY
      if (!request.has_ready_req()) {
        continue;
      }

      // Use the information in each READY message to build up the translation maps
      const auto& conn_id = ready_msg.GetIdentity();
      const auto& ready = request.ready_req();
      const auto& addr = ready.ip_address();
      const auto& slog_id = ready.slog_id();
      auto slog_id_str = SlogIdToString(slog_id);

      if (needed_slog_ids.count(slog_id_str) == 0) {
        continue;
      }

      LOG(INFO) << "Received READY message from " << addr 
                << " (rep: " << slog_id.replica() 
                << ", part: " << slog_id.partition() << ")";

      slog_id_to_address_[slog_id_str] = addr;
      connection_id_to_slog_id_[conn_id] = slog_id_str;

      needed_slog_ids.erase(slog_id_str);
    }

    if (needed_slog_ids.empty()) {
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

  // Set up poll items
  vector<zmq::pollitem_t> items;
  items.reserve(channels_.size() + 1);

  vector<std::string> channel_names;
  channel_names.reserve(channels_.size());

  for (const auto& pair : channels_) {
    items.push_back(pair.second->GetPollItem());
    channel_names.push_back(pair.first);
  }
  // NOTE: always push this item at the end so that the channel indices
  // start from 0
  items.push_back({static_cast<void*>(router_), 0, ZMQ_POLLIN, 0});

  while (running_) {
    // Wait until a message arrived at one of the sockets
    zmq::poll(items, BROKER_POLL_TIMEOUT_MS);

    // Router received a message
    if (items.back().revents & ZMQ_POLLIN) {

      MMessage message(router_);

      // Broker the message to the target channel
      const auto& target_channel = message.GetChannel();
      if (channels_.count(target_channel) == 0) {
        LOG(ERROR) << "Unknown channel: \"" << target_channel 
                   << "\". Dropping the message";
      } else {
        // Translate connection id to slog id (replica, partition) 
        // before sending to the channel
        const auto& conn_id = message.GetIdentity();
        message.SetIdentity(connection_id_to_slog_id_[conn_id]);
        channels_[target_channel]->SendToListener(message);
      }
    }

    for (size_t i = 0; i < items.size() - 1; i++) {
      // A channel sent a message. Pass this message to the router to send it out.
      if (items[i].revents & ZMQ_POLLIN) {
        const auto& name = channel_names[i];
        MMessage message;
        channels_[name]->ReceiveFromListener(message);

        // Remove the identity part of the message before sending out to a DEALER socket
        const auto& slog_id = message.GetIdentity();
        const auto& addr = slog_id_to_address_[slog_id];
        message.SetIdentity("");
        message.Send(*address_to_socket_[addr]);
      }
    }
  }
}

} // namespace slog