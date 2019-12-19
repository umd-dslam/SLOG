#pragma once

#include "proto/internal.pb.h"

namespace slog {

proto::Request MakeEchoRequest(const std::string& data);
proto::Response MakeEchoResponse(const std::string& data);

} // namespace slog