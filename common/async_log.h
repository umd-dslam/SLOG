#pragma once

#include <sstream>
#include <unordered_map>

namespace slog {

template<typename T>
class AsyncLog {
public:
  AsyncLog(uint32_t start_from = 0) : next_(start_from) {}
  
  void Insert(uint32_t position, T item) {
    if (position < next_) {
      return;
    }
    if (log_.count(position) > 0) {
      std::ostringstream os;
      os << "Log position " << position << " has already been taken";
      throw std::runtime_error(os.str());
    }
    log_[position] = item;
  }

  bool HasNext() const {
    return log_.count(next_) > 0;
  }

  const T& Peek() {
    return log_.at(next_);
  }

  std::pair<T, uint32_t> Next() {
    if (!HasNext()) {
      throw std::runtime_error("Next item does not exist");
    }
    auto position = next_;
    next_++;

    T result = std::move(log_[position]);
    log_.erase(position);
    return std::make_pair(position, std::move(result));
  }

private:
  std::unordered_map<uint32_t, T> log_;
  uint32_t next_;
};

} // namespace slog