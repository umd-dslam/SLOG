#include "module/server.h"

#include "proto/internal.pb.h"

namespace slog {

Server::Server(Channel* listener) 
  : Module(listener) {}

void Server::HandleMessage(MMessage message) {
  internal::Request request;
  if (message.GetProto(request)) {
    waiting_requests_[request.stream_id()] = std::move(message);
  } else {
    
  }
}

void Server::PostProcessing() {

}

} // namespace slog