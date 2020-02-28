#include "ticker.h"

namespace slog {

const string Ticker::ENDPOINT("inproc://ticker");

Ticker::Ticker(zmq::context_t& context, uint32_t ticks_per_sec)
    : socket_(context, ZMQ_PUB),
      ticks_per_sec_(ticks_per_sec) {
  socket_.setsockopt(ZMQ_LINGER, 0);
  sleep_us_ = std::chrono::microseconds(1000 * 1000 / ticks_per_sec_);
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