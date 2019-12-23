#pragma once

#include <thread>
#include <vector>

#include "connection/channel.h"

namespace slog {

class Module {
public:
  Module();
  Module(const Module&) = delete;
  const Module& operator=(const Module&) = delete;

  virtual ~Module();

  std::thread StartInNewThread();
  void Start();

protected:
  virtual void Loop() = 0;

private:
  void Run();
  std::atomic<bool> running_;
};

class ChanneledModule : public Module {
public:
  ChanneledModule(Channel* listener);
  
protected:
  void Send(const MMessage& message);

  virtual void HandleMessage(MMessage message) = 0;

  virtual void HandlePollTimedOut() {};

  virtual void PostProcessing() {};

private:
  void Loop() final;

  std::unique_ptr<Channel> listener_;
  zmq::pollitem_t poll_item_;
};

} // namespace slog