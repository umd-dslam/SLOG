#pragma once

#include <vector>

#include "common/configuration.h"
#include "proto/internal.pb.h"

using std::string;

namespace slog {

using ConfigVec = std::vector<std::shared_ptr<Configuration>>;

internal::Request MakeEchoRequest(const string& data);
internal::Response MakeEchoResponse(const string& data);

ConfigVec MakeTestConfigurations(
    string&& prefix,
    int num_replicas, 
    int num_partitions);

} // namespace slog