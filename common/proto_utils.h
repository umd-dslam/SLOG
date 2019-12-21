#pragma once

#include "proto/slog_identifier.pb.h"

namespace slog {
proto::SlogIdentifier MakeSlogIdentifier(uint32_t replica, uint32_t partition);
}