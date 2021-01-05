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

PollResult Poller::Wait() {
  // Compute the time that we need to wait until the next event
  auto shortest_timeout = poll_timeout_;
  auto now = Clock::now();
  for (auto& ev : time_events_) {
    if (ev.when <= now) {
      shortest_timeout = 0us;
      break;
    } else if (ev.when - now < shortest_timeout) {
      shortest_timeout = duration_cast<microseconds>(ev.when - now);
    }
  }

  // Wait until the next time event or some timeout
  PollResult result;
  result.num_zmq_events = zmq::poll(poll_items_, duration_cast<milliseconds>(shortest_timeout));

  // Process and clean up triggered time events
  now = Clock::now();
  for (auto it = time_events_.begin(); it != time_events_.end();) {
    if (it->when <= now) {
      result.time_events.push_back(it->data);
      it = time_events_.erase(it);
    } else {
      ++it;
    }
  }

  return result;
}

bool Poller::is_socket_ready(size_t i) const { return poll_items_[i].revents & ZMQ_POLLIN; }

void Poller::AddTimeEvent(microseconds timeout, void* data) {
  time_events_.push_back({.when = Clock::now() + timeout, .data = data});
}

}  // namespace slog