#pragma once

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <zmq.hpp>

namespace slog {

void InitializeService(int argc, char* argv[]);

} // namespace slog