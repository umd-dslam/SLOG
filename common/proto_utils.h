#pragma once

#include "proto/slog_identifier.pb.h"

namespace slog {

proto::SlogIdentifier MakeSlogId(uint32_t replica, uint32_t partition);
std::string SlogIdToString(const proto::SlogIdentifier slog_id);

}