#include "ticker.h"

namespace slog {

const string Ticker::ENDPOINT("inproc://ticker");

zmq::socket_t Ticker::Subscribe(zmq::context_t& context) {
  zmq::socket_t socket(context, ZMQ_SUB);
  socket.connect(Ticker::ENDPOINT);
  // Subscribe to any message
  socket.setsockopt(ZMQ_SUBSCRIBE, "", 0);
  return socket;
}

Ticker::Ticker(zmq::context_t& context, std::chrono::milliseconds tick_period_ms)
    : socket_(context, ZMQ_PUB) {
  socket_.setsockopt(ZMQ_LINGER, 0);
  sleep_us_ = std::chrono::duration_cast<milliseconds>(tick_period_ms);
}

Ticker::Ticker(zmq::context_t& context, uint32_t ticks_per_sec)
    : socket_(context, ZMQ_PUB) {
  socket_.setsockopt(ZMQ_LINGER, 0);
  sleep_us_ = std::chrono::microseconds(1000 * 1000 / ticks_per_sec);
}

void Ticker::SetUp() {
  socket_.bind(ENDPOINT);
}

void Ticker::Loop() {
  zmq::message_t msg;
  socket_.send(msg, zmq::send_flags::dontwait);
  std::this_thread::sleep_for(sleep_us_);
}

} // namespace slog