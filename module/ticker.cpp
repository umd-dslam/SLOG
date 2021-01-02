#include "ticker.h"

#include <cmath>

using std::string;

namespace slog {

const string Ticker::kEndpoint("inproc://ticker");

zmq::socket_t Ticker::Subscribe(zmq::context_t& context) {
  zmq::socket_t socket(context, ZMQ_SUB);
  socket.connect(Ticker::kEndpoint);
  // Subscribe to any message
  socket.set(zmq::sockopt::subscribe, "");
  return socket;
}

Ticker::Ticker(zmq::context_t& context, std::chrono::milliseconds tick_period_ms)
    : Module("Ticker"), socket_(context, ZMQ_PUB) {
  sleep_ms_ = duration_cast<DurationFloatMs>(tick_period_ms);
}

Ticker::Ticker(zmq::context_t& context, uint32_t ticks_per_sec) : Module("Ticker"), socket_(context, ZMQ_PUB) {
  sleep_ms_ = DurationFloatMs(1000.0 / ticks_per_sec);
}

void Ticker::SetUp() { socket_.bind(kEndpoint); }

void Ticker::Loop() {
  zmq::message_t msg;
  auto deadline = std::chrono::high_resolution_clock::now() + sleep_ms_;

  socket_.send(msg, zmq::send_flags::dontwait);
  std::this_thread::sleep_until(deadline);
}

}  // namespace slog