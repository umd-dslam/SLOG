#pragma once

#include "proto/internal.pb.h"

namespace slog {

using internal::SlogIdentifier;

SlogIdentifier MakeSlogId(uint32_t replica, uint32_t partition);
std::string SlogIdToString(const SlogIdentifier slog_id);

}