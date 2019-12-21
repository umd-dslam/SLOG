#include "common/test_utils.h"

namespace slog {

proto::Request MakeEchoRequest(const std::string& data) {
  proto::Request request;
  auto echo = request.mutable_echo_req();
  echo->set_data(data);
  return request;
}

proto::Response MakeEchoResponse(const std::string& data) {
  proto::Response response;
  auto echo = response.mutable_echo_res();
  echo->set_data(data);
  return response;
}

} // namespace slog