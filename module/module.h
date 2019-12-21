#pragma once

#include <thread>

#include "connection/channel.h"
#include "proto/request.pb.h"
#include "proto/response.pb.h"

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
  void Send(const proto::Request& request);
  void Send(const proto::Response& response);

  virtual void HandleRequest(const proto::Request& request) = 0;
  virtual void HandleResponse(const proto::Response& response) = 0;
  virtual void PostProcessing() {};

private:
  void Run();

  void Send(const MMessage& message);

  std::unique_ptr<ChannelListener> listener_;
  std::atomic<bool> running_;
};

} // namespace slog