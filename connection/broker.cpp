#include "connection/broker.h"

namespace slog {

Broker::Broker(
    std::shared_ptr<Configuration> config, 
    zmq::context_t& context) 
  : config_(config),
    router_(context, ZMQ_ROUTER),
    running_(true) {
  channels_.emplace_back(context, "server");
  thread_ = std::thread(&Broker::Run, this);
}

Broker::~Broker() {
  running_ = false;
  thread_.join();
}

void Broker::Run() {
  std::string endpoint("tcp://*:");
  endpoint.append(std::to_string(config_->GetBrokerPort()));
  router_.bind(endpoint);

  // Set up poll items
  std::vector<zmq::pollitem_t> items = {  
    { static_cast<void*>(router_), 0, ZMQ_POLLIN, 0 },
  };
  for (auto& channel : channels_) {
    items.push_back(channel.GetPollItem());
  }

  while (running_) {
    zmq::message_t message;
    // Wait until a message arrived at one of the sockets
    zmq::poll(&items[0], 4, -1 /* no timeout */);

    // Router received a request
    if (items[0].revents & ZMQ_POLLIN) {
      
    }

    // A channel replied to a request
    for (size_t i = 1; i < items.size(); i++) {
      if (items[i].revents & ZMQ_POLLIN) {
        int channel_index = i - 1;

      }
    }
  }
}

} // namespace slog