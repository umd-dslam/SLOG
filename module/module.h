#pragma once

#include <thread>
#include <vector>

#include "connection/channel.h"

using std::unique_ptr;

namespace slog {

class Module {
public:
  Module() {};
  Module(const Module&) = delete;
  const Module& operator=(const Module&) = delete;
  virtual ~Module() {}

  virtual void SetUp() {};
  virtual void Loop() = 0;
};

class ChanneledModule : public Module {
public:
  ChanneledModule(Channel* listener, long poll_timeout_ms = 1000);

protected:
  void Send(const MMessage& message);

  virtual void HandleMessage(MMessage message) = 0;

  virtual void HandlePollTimedOut() {};

  virtual void PostProcessing() {};

private:
  void Loop() final;

  std::unique_ptr<Channel> listener_;
  zmq::pollitem_t poll_item_;
  long poll_timeout_ms_;
};

class ModuleRunner {
public:
  ModuleRunner(Module* module);
  ~ModuleRunner();

  void Start();
  void StartInNewThread();

private:
  void Run();

  unique_ptr<Module> module_;
  std::thread thread_;
  std::atomic<bool> running_;
};

template<typename T, typename... Args>
inline unique_ptr<ModuleRunner>
MakeRunnerFor(Args&&... args)
{
  typedef typename std::remove_cv<T>::type T_nc;
  return std::make_unique<ModuleRunner>(new T_nc(std::forward<Args>(args)...));
}

} // namespace slog