#include "connection/broker.h"

#include <sstream>

#include "common/mmessage.h"

namespace slog {

const std::string Broker::SERVER_CHANNEL = "server";

Broker::Broker(
    std::shared_ptr<Configuration> config, 
    std::shared_ptr<zmq::context_t> context) 
  : config_(config),
    router_(*context, ZMQ_ROUTER),
    running_(true) {
  channels_.emplace_back(new Channel(context, SERVER_CHANNEL));

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

void Broker::Run() {
  std::stringstream endpoint;
  endpoint << "tcp://*:" << config_->GetBrokerPort();
  router_.bind(endpoint.str());

  // Set up poll items
  std::vector<zmq::pollitem_t> items = {  
    { static_cast<void*>(router_), 0, ZMQ_POLLIN, 0 },
  };
  for (auto& channel : channels_) {
    items.push_back(channel->GetPollItem());
  }

  while (running_) {
    // Wait until a message arrived at one of the sockets
    zmq::poll(items);

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