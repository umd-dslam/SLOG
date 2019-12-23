#pragma once

#include <thread>
#include <vector>

#include "connection/channel.h"

namespace slog {

class Module {
public:
  Module(Channel* listener);
  
  Module(const Module&) = delete;
  const Module& operator=(const Module&) = delete;

  virtual ~Module();

  std::thread StartInNewThread();
  void Start();

protected:
  void AddPollItem(zmq::pollitem_t&& poll_item);

  void Send(const MMessage& message);

  virtual void HandleMessage(MMessage message) = 0;

  virtual void HandlePollTimedOut() {};

  virtual void PostProcessing() {};

private:
  void Run();
  std::unique_ptr<Channel> listener_;
  std::atomic<bool> running_;
  std::vector<zmq::pollitem_t> poll_items_;
};

} // namespace slog