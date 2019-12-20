#include "connection/broker.h"

#include <sstream>

#include "common/mmessage.h"
#include "proto/internal.pb.h"

namespace slog {

const long BROKER_POLL_TIMEOUT_US = 10 * 1000;
const std::string Broker::SERVER_CHANNEL = "server";

Broker::Broker(
    std::shared_ptr<Configuration> config, 
    std::shared_ptr<zmq::context_t> context) 
  : config_(config),
    router_(*context, ZMQ_ROUTER),
    running_(true) {
  channels_.emplace_back(new Channel(context, SERVER_CHANNEL));

  for (const auto& addr : config->GetAllAddresses()) {
    auto socket = std::make_unique<zmq::socket_t>(*context, ZMQ_DEALER);
    address_to_socket_[addr] = std::move(socket);
  }

  thread_ = std::thread(&Broker::Run, this);
}

Broker::~Broker() {
  running_ = false;
  thread_.join();
}

ChannelListener* Broker::GetChannelListener(const std::string& name) {
  auto found = std::find_if(
      channels_.begin(), 
      channels_.end(), 
      [&name](const std::unique_ptr<Channel>& channel) {
        return channel->GetName() == name;
      });
  if (found == channels_.end()) {
    return nullptr;
  }
  return (*found)->GetListener();
}

void Broker::InitializeConnection() {
  std::stringstream endpoint;
  endpoint << "tcp://*:" << config_->GetBrokerPort();
  router_.bind(endpoint.str());

  proto::Request request;
  auto ready = request.mutable_ready();
  ready->set_ip_address(config_->GetLocalAddress());
  *ready->mutable_slog_identifier() = config_->GetLocalSlogIdentifier();
  MMessage ready_msg(request);

  for (const auto& pair : address_to_socket_) {
    const auto& addr = pair.first;
    const auto& socket = pair.second;
    endpoint.clear();
    endpoint << "tcp://" << addr << config_->GetBrokerPort();
    socket->connect(endpoint.str());

    ready_msg.Send(*socket);
  }

  zmq::pollitem_t item = { static_cast<void*>(router_), 0, ZMQ_POLLIN, 0 };

  while (running_) {
    zmq::poll(&item, 1, BROKER_POLL_TIMEOUT_US);
    if (item.revents & ZMQ_POLLIN) {
      ready_msg.Receive(router_);
    }
  }
}

void Broker::Run() {

  InitializeConnection();

  // Set up poll items
  std::vector<zmq::pollitem_t> items = {  
    { static_cast<void*>(router_), 0, ZMQ_POLLIN, 0 },
  };
  for (auto& channel : channels_) {
    items.push_back(channel->GetPollItem());
  }

  while (running_) {
    // Wait until a message arrived at one of the sockets
    zmq::poll(items, BROKER_POLL_TIMEOUT_US);

    // Router received a request
    if (items[0].revents & ZMQ_POLLIN) {
      MMessage message(router_);
      // Send to all channels
      for (auto& channel : channels_) {
        channel->SendToListener(message);
      }
    }

    // A channel replied to a request. Pass this message to
    // the router to send it out.
    for (size_t i = 1; i < items.size(); i++) {
      if (items[i].revents & ZMQ_POLLIN) {
        int channel_index = i - 1;
        auto& channel = channels_[channel_index];
        MMessage message;
        channel->ReceiveFromListener(message);
        message.Send(router_);
      }
    }
  }
}

} // namespace slog