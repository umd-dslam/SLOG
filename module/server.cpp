#include "module/server.h"

#include "proto/request.pb.h"
#include "proto/response.pb.h"

namespace slog {

Server::Server(ChannelListener* listener) 
  : Module(listener) {}

void Server::HandleMessage(MMessage message) {
  if (!message.IsResponse()) {
    proto::Request request;
    message.ToRequest(request);
    waiting_requests_[request.stream_id()] = std::move(request);
  } else {
    
  }
}

void Server::PostProcessing() {

}

} // namespace slog