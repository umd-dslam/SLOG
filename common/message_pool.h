#pragma once

#include <vector>

namespace slog {

template <typename T>
class MessagePool {
 public:
  MessagePool() : MessagePool(0) {}

  MessagePool(size_t size) {
    messages_.reserve(size);
    for (size_t i = 0; i < size; i++) {
      messages_.push_back(new T());
    }
  }

  ~MessagePool() {
    for (auto msg : messages_) {
      delete msg;
    }
    messages_.clear();
  }

  T* Acquire() {
    if (messages_.empty()) {
      return new T();
    }
    auto tmp = messages_.back();
    messages_.pop_back();
    return tmp;
  }

  void Return(T* msg) {
    if (msg != nullptr) {
      messages_.push_back(msg);
    }
  }

  size_t size() const { return messages_.size(); }

 private:
  MessagePool(const MessagePool&) = delete;
  MessagePool& operator=(const MessagePool&) = delete;

  std::vector<T*> messages_;
};

template <typename T>
class ReusableMessage {
 public:
  ReusableMessage() = default;

  explicit ReusableMessage(MessagePool<T>* pool) : pool_(pool) {
    msg_ = nullptr;
    if (pool_ != nullptr) {
      msg_ = pool_->Acquire();
    }
  }

  ~ReusableMessage() {
    if (pool_ != nullptr) {
      pool_->Return(msg_);
      msg_ = nullptr;
    }
  }

  ReusableMessage(const ReusableMessage<T>& other) : pool_(other.pool_) {
    msg_ = nullptr;
    if (pool_ != nullptr) {
      msg_ = pool_->Acquire();
      *msg_ = *other.msg_;
    }
  }

  ReusableMessage(ReusableMessage<T>&& other) noexcept : msg_(other.msg_), pool_(other.pool_) {
    other.msg_ = nullptr;
    other.pool_ = nullptr;
  }

  ReusableMessage<T>& operator=(ReusableMessage<T> other) noexcept {
    std::swap(msg_, other.msg_);
    std::swap(pool_, other.pool_);
    return *this;
  }

  inline T* get() const { return msg_; }

 private:
  // Invariant: If pool_ is nullptr then msg_ must also be nullptr
  T* msg_ = nullptr;
  MessagePool<T>* pool_ = nullptr;
};

}  // namespace slog