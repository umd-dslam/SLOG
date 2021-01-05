#pragma once

#include <list>
#include <vector>
#include <zmq.hpp>

namespace slog {

struct PollResult {
  int num_zmq_events;
  std::vector<void*> time_events;
};

class Poller {
 public:
  Poller(std::chrono::microseconds timeout);

  PollResult Wait();

  void PushSocket(zmq::socket_t& socket);

  bool is_socket_ready(size_t i) const;

  void AddTimeEvent(std::chrono::microseconds timeout, void* data);

 private:
  using Clock = std::chrono::steady_clock;
  using TimePoint = Clock::time_point;
  struct TimeEvent {
    TimePoint when;
    void* data;
  };

  std::chrono::microseconds poll_timeout_;
  std::vector<zmq::pollitem_t> poll_items_;
  std::list<TimeEvent> time_events_;
};

}  // namespace slog