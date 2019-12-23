#pragma once

#include "proto/internal.pb.h"

namespace slog {

internal::Request MakeEchoRequest(const std::string& data);
internal::Response MakeEchoResponse(const std::string& data);

} // namespace slog