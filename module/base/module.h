#pragma once

#include <atomic>
#include <thread>
#include <vector>

#include "connection/channel.h"

using std::shared_ptr;
using std::unique_ptr;
using std::string;

namespace slog {

/**
 * An interface for a module in SLOG. Most modules only need
 * to connect to a single channel from the broker so they usually
 * extend from BasicModule. Extending from this class is
 * only needed if a module needs more than one socket and
 * more flexibility in implementation (for example: Server)
 * 
 * A module only contains the instructions for what to run.
 * To actually run, it has to be coupled with a ModuleRunner.
 */
class Module {
public:
  Module() {};
  Module(const Module&) = delete;
  const Module& operator=(const Module&) = delete;
  virtual ~Module() {}

  /**
   * To be called before the main loop. This gives a chance to perform 
   * all neccessary one-time initialization.
   */
  virtual void SetUp() {};

  /**
   * Contains the actions to be perform in one iteration of the main loop
   */
  virtual void Loop() = 0;
};

/**
 * A ModuleRunner executes a Module. Its execution can either live in
 * a new thread or in the same thread as its caller.
 */
class ModuleRunner {
public:
  ModuleRunner(const shared_ptr<Module>& module);
  ~ModuleRunner();

  void Start();
  void StartInNewThread();

private:
  void Run();

  shared_ptr<Module> module_;
  std::thread thread_;
  std::atomic<bool> running_;
};

/**
 * Helper function for creating a ModuleRunner.
 */
template<typename T, typename... Args>
inline unique_ptr<ModuleRunner>
MakeRunnerFor(Args&&... args)
{
  return std::make_unique<ModuleRunner>(
      std::make_shared<T>(std::forward<Args>(args)...));
}

/**
 * A base class for module that holds a channel
 */
class ChannelHolder {
public:
  ChannelHolder(unique_ptr<Channel>&& channel_);

  /**
   * Send a request or response to a given channel of a given machine
   * @param request_or_response Request or response to be sent
   * @param to_machine_id Id of the machine that this message is sent to
   * @param to_channel Channel on the machine that this message is sent to
   * @param has_more Setting this to true signals the receiver (broker) to
   *                 receive the next message in succession.
   */
  void Send(
      const google::protobuf::Message& request_or_response,
      const string& to_machine_id,
      const string& to_channel,
      bool has_more = false);

  /**
   * Send a request or response to a given channel on this same machine
   * @param request_or_response Request or response to be sent
   * @param to_channel Channel to send to
   * @param has_more Setting this to true signals the receiver (broker) to
   *                 receive the next message in succession.
   */
  void SendSameMachine(
      const google::protobuf::Message& request_or_response,
      const string& to_channel,
      bool has_more = false);

  /**
   * Send a request or response to the same channel on another machine
   * @param request_or_response Request or response to be sent
   * @param to_machine_id Machine to send to
   * @param has_more Setting this to true signals the receiver (broker) to
   *                 receive the next message in succession.
   */
  void SendSameChannel(
      const google::protobuf::Message& request_or_response,
      const string& to_machine_id,
      bool has_more = false);

  /**
   * Send a message to the destination specified in the message
   * @param message The message to be sent
   * @param has_more Setting this to true signals the receiver (broker) to
   *                 receive the next message in succession.
   */
  void Send(MMessage&& message, bool has_more = false);

  zmq::pollitem_t GetChannelPollItem() const;

  void ReceiveFromChannel(MMessage& message);

private:
  unique_ptr<Channel> channel_;
};

} // namespace slog