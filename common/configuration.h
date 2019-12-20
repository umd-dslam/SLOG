#pragma once

#include <string>
#include <unordered_map>
#include "common/types.h"
#include "proto/config.pb.h"

using std::unordered_map;
using std::vector;

namespace slog {

class Configuration {
public:
  static std::shared_ptr<Configuration> FromFile(
      const std::string& file_path,
      const std::string& local_address,
      const MachineIdentifier& local_identifier);

  uint32_t GetBrokerPort() const;

  Configuration(
      proto::Configuration&& config,
      const std::string& local_address,
      const MachineIdentifier& local_identifier);

private:
  uint32_t broker_port_;
  std::vector<std::string> all_addresses_;
  std::string local_address_;
  MachineIdentifier local_identifier_;
};

} // namespace slog