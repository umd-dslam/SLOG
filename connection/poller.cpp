#include "connection/poller.h"

using namespace std::chrono;

using std::vector;

namespace slog {

Poller::Poller(microseconds timeout) : poll_timeout_(timeout) {}

void Poller::PushSocket(zmq::socket_t& socket) {
  poll_items_.push_back({
      socket.handle(), 0, /* fd */
      ZMQ_POLLIN, 0       /* revent */
  });
}

int Poller::Wait() {
  // Compute the time that we need to wait until the next event
  auto shortest_timeout = poll_timeout_;
  auto now = Clock::now();
  for (auto& ev : timed_callbacks_) {
    if (ev.when <= now) {
      shortest_timeout = 0us;
      break;
    } else if (ev.when - now < shortest_timeout) {
      shortest_timeout = duration_cast<microseconds>(ev.when - now);
    }
  }

  // Wait until the next time event or some timeout
  auto res = zmq::poll(poll_items_, duration_cast<milliseconds>(shortest_timeout));

  // Process and clean up triggered callbacks
  now = Clock::now();
  for (auto it = timed_callbacks_.begin(); it != timed_callbacks_.end();) {
    if (it->when <= now) {
      it->callback();
      it = timed_callbacks_.erase(it);
    } else {
      ++it;
    }
  }

  return res;
}

bool Poller::is_socket_ready(size_t i) const { return poll_items_[i].revents & ZMQ_POLLIN; }

void Poller::AddTimedCallback(microseconds timeout, const std::function<void()>& cb) {
  timed_callbacks_.push_back({.when = Clock::now() + timeout, .callback = cb});
}

}  // namespace slog