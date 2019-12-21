#include "connection/broker.h"

#include <sstream>
#include <unordered_set>

#include <glog/logging.h>

#include "common/mmessage.h"
#include "proto/internal.pb.h"

using std::pair;
using std::unordered_set;

namespace slog {

const long BROKER_POLL_TIMEOUT_MS = 1000;

Broker::Broker(
    std::shared_ptr<Configuration> config, 
    std::shared_ptr<zmq::context_t> context) 
  : config_(config),
    router_(*context, ZMQ_ROUTER),
    running_(true) {
  // Set ZMQ_LINGER to 0 to discard all pending messages on shutdown.
  // Otherwise, it would hang indefinitely until the messages are sent.
  router_.setsockopt(ZMQ_LINGER, 0);
  for (const auto& addr : config->GetAllAddresses()) {
    auto socket = std::make_unique<zmq::socket_t>(*context, ZMQ_DEALER);
    socket->setsockopt(ZMQ_LINGER, 0);
    address_to_socket_[addr] = std::move(socket);
  }

  // Initialize all channels
  for (
      size_t c = 0; 
      c < static_cast<size_t>(ChannelName::END); 
      c++) {
    channels_.emplace_back(new Channel(context, static_cast<ChannelName>(c)));
  }

  thread_ = std::thread(&Broker::Run, this);
}

Broker::~Broker() {
  running_ = false;
  thread_.join();
}

ChannelListener* Broker::GetChannelListener(ChannelName name) {
  return channels_[static_cast<size_t>(name)]->GetListener();
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
  auto ready = request.mutable_ready();
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
      proto::SlogIdentifier slog_id;
      slog_id.set_replica(rep);
      slog_id.set_partition(part);
      needed_slog_ids.insert(slog_id.SerializeAsString());
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
      if (!request.has_ready()) {
        continue;
      }

      // Use the information in each READY message to build up the translation maps
      const auto& conn_id = ready_msg.GetIdentity();
      const auto& ready = request.ready();
      const auto& addr = ready.ip_address();
      const auto& slog_id = ready.slog_id();
      auto slog_id_str = slog_id.SerializeAsString();

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

  for (const auto& channel : channels_) {
    items.push_back(channel->GetPollItem());
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
      auto target_channel = message.GetChannel();
      if (target_channel == ChannelName::END) {
        LOG(ERROR) << "Invalid channel \"END\". Dropping the message";
      } else {
        // Translate connection id to slog id (replica, partition) 
        // before sending to the channel
        const auto& conn_id = message.GetIdentity();
        message.SetIdentity(connection_id_to_slog_id_[conn_id]);
        auto& channel = channels_[static_cast<size_t>(target_channel)];
        channel->SendToListener(message);
      }
    }

    for (size_t c = 0; c < items.size() - 1; c++) {
      // A channel sent a message. Pass this message to the router to send it out.
      if (items[c].revents & ZMQ_POLLIN) {
        MMessage message;
        channels_[c]->ReceiveFromListener(message);

        // Remove the identity part of the message before sending out to a DEALER socket
        const auto& slog_id = message.GetIdentity();
        const auto& addr = slog_id_to_address_[slog_id];
        message.RemoveIdentity();
        message.Send(*address_to_socket_[addr]);
      }
    }
  }
}

} // namespace slog