#pragma once

#include "proto/request.pb.h"
#include "proto/response.pb.h"

namespace slog {

proto::Request MakeEchoRequest(const std::string& data);
proto::Response MakeEchoResponse(const std::string& data);

} // namespace slog