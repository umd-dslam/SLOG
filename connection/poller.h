#pragma once

#include <functional>
#include <list>
#include <optional>
#include <vector>
#include <zmq.hpp>

namespace slog {

class Poller {
 public:
  Poller(std::optional<std::chrono::microseconds> timeout);

  int Wait();

  void PushSocket(zmq::socket_t& socket);

  bool is_socket_ready(size_t i) const;

  void AddTimedCallback(std::chrono::microseconds timeout, std::function<void()>&& cb);

 private:
  using Clock = std::chrono::steady_clock;
  using TimePoint = Clock::time_point;
  struct TimedCallback {
    TimePoint when;
    std::function<void()> callback;
  };

  std::optional<std::chrono::microseconds> poll_timeout_;
  std::vector<zmq::pollitem_t> poll_items_;
  std::list<TimedCallback> timed_callbacks_;
};

}  // namespace slog