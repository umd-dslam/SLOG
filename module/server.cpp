#include "module/server.h"

namespace slog {

Server::Server(ChannelListener* listener) 
  : Module(listener) {}

void Server::HandleRequest(const proto::Request& request) {
  
}

void Server::HandleResponse(const proto::Response& response) {

}

void Server::PostProcessing() {

}

} // namespace slog