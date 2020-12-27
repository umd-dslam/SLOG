#include "ticker.h"

using std::string;

namespace slog {

const string Ticker::ENDPOINT("inproc://ticker");

zmq::socket_t Ticker::Subscribe(zmq::context_t& context) {
  zmq::socket_t socket(context, ZMQ_SUB);
  socket.connect(Ticker::ENDPOINT);
  // Subscribe to any message
  socket.set(zmq::sockopt::subscribe, "");
  return socket;
}

Ticker::Ticker(zmq::context_t& context, std::chrono::milliseconds tick_period_ms)
    : Module("Ticker"), socket_(context, ZMQ_PUB) {
  socket_.set(zmq::sockopt::linger, 0);
  sleep_us_ = std::chrono::duration_cast<milliseconds>(tick_period_ms);
}

Ticker::Ticker(zmq::context_t& context, uint32_t ticks_per_sec) : Module("Ticker"), socket_(context, ZMQ_PUB) {
  socket_.set(zmq::sockopt::linger, 0);
  sleep_us_ = std::chrono::microseconds(1000 * 1000 / ticks_per_sec);
}

void Ticker::SetUp() { socket_.bind(ENDPOINT); }

void Ticker::Loop() {
  zmq::message_t msg;
  auto deadline = std::chrono::steady_clock::now() + sleep_us_;

  socket_.send(msg, zmq::send_flags::dontwait);
  std::this_thread::sleep_until(deadline);
}

}  // namespace slog