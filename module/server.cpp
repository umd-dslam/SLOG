#include "module/server.h"

#include "proto/request.pb.h"
#include "proto/response.pb.h"

namespace slog {

Server::Server(Channel* listener) 
  : Module(listener) {}

void Server::HandleMessage(MMessage message) {
  proto::Request request;
  if (message.GetProto(request)) {
    waiting_requests_[request.stream_id()] = std::move(message);
  } else {
    
  }
}

void Server::PostProcessing() {

}

} // namespace slog