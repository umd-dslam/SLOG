#pragma once

#include <thread>

#include "connection/channel.h"

namespace slog {

class Module {
public:
  Module(ChannelListener* listener);
  
  Module(const Module&) = delete;
  const Module& operator=(const Module&) = delete;

  virtual ~Module();

  std::thread StartInNewThread();
  void Start();

protected:
  void Send(const MMessage& message);

  virtual void HandleMessage(MMessage message) = 0;

  virtual void PostProcessing() {};

private:
  void Run();
  std::unique_ptr<ChannelListener> listener_;
  std::atomic<bool> running_;
};

} // namespace slog