#include "module/workload.h"

#include <random>

#include "proto/request.pb.h"
#include "proto/response.pb.h"

namespace slog {

Workload::Workload(ChannelListener* listener) 
  : Module(listener),
    rand_eng_(std::random_device{}()),
    dist_(10, 100) {}

void Workload::HandleMessage(MMessage message) {
  if (!message.IsResponse()) {
    proto::Request request;
    message.ToRequest(request);
    auto stream_id = request.stream_id();
    waiting_requests_[stream_id] = std::move(message);

    response_time_.insert(std::make_pair(
        Clock::now() + milliseconds(dist_(rand_eng_)),
        stream_id));
  }
}

void Workload::PostProcessing() {
  auto top = response_time_.begin();
  if (top->first <= Clock::now()) {
    proto::Response response;
    response.set_stream_id(top->second);
    auto& msg = waiting_requests_[top->second];
    msg.SetResponse(response);
    msg.SetChannel("workload");
  }
}

} // namespace slog