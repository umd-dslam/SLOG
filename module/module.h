#pragma once

#include <atomic>
#include <thread>
#include <vector>

#include "connection/channel.h"

using std::unique_ptr;
using std::string;

namespace slog {

/**
 * An interface for a module in SLOG. Most modules only need
 * to connect to a single channel from the broker so they usually
 * extend from BasicModule. Extending from this class is
 * only needed if a module needs more than one socket or
 * needs more flexibility in implementation (for example: Server)
 */
class Module {
public:
  Module() {};
  Module(const Module&) = delete;
  const Module& operator=(const Module&) = delete;
  virtual ~Module() {}

  virtual void SetUp() {};
  virtual void Loop() = 0;
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

class ChannelHolder {
public:
  ChannelHolder(Channel* listener_);

protected:
  void Send(
      const google::protobuf::Message& request_or_response,
      const string& to_machine_id,
      const string& to_channel);

  void Send(
      const google::protobuf::Message& request_or_response,
      const string& to_channel);

  void Send(MMessage&& message);

  zmq::pollitem_t GetChannelPollItem() const;

  void ReceiveFromChannel(MMessage& message);

private:
  unique_ptr<Channel> listener_;
};

} // namespace slog