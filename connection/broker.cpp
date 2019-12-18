#include "connection/broker.h"

namespace slog {

const std::string Broker::SERVER_ENDPOINT = "inproc://server";
const std::string Broker::SEQUENCER_ENDPOINT = "inproc://sequencer";
const std::string Broker::SCHEDULER_ENDPOINT = "inproc://scheduler";

Broker::Broker(
    std::shared_ptr<Configuration> config, 
    zmq::context_t& context) 
  : config_(config),
    router_(context, ZMQ_ROUTER),
    server_channel_(context, ZMQ_PAIR),
    sequencer_channel_(context, ZMQ_PAIR),
    scheduler_channel_(context, ZMQ_PAIR),
    running_(true) {
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

  server_channel_.bind(SERVER_ENDPOINT);
  sequencer_channel_.bind(SEQUENCER_ENDPOINT);
  scheduler_channel_.bind(SCHEDULER_ENDPOINT);

  zmq::pollitem_t items[] = {
    { static_cast<void*>(router_), 0, ZMQ_POLLIN, 0 },
    { static_cast<void*>(server_channel_), 0, ZMQ_POLLIN, 0 },
    { static_cast<void*>(sequencer_channel_), 0, ZMQ_POLLIN, 0 },
    { static_cast<void*>(scheduler_channel_), 0, ZMQ_POLLIN, 0 },
  };

  while (running_) {
    zmq::message_t message;
    zmq::poll(&items[0], 4, -1 /* no timeout */);

    // router received a request
    if (items[0].revents & ZMQ_POLLIN) {

    }

    // server replied to a request
    if (items[1].revents & ZMQ_POLLIN) {

    }

    // sequencer replied to a request
    if (items[2].revents & ZMQ_POLLIN) {
      
    }

    // scheduler replied to a request
    if (items[3].revents & ZMQ_POLLIN) {
      
    }
  }
}

} // namespace slog