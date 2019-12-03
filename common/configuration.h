#pragma once

#include <string>
#include <unordered_map>
#include "proto/config.pb.h"

using std::unordered_map;
using std::vector;

namespace slog {

class Configuration {
public:
  static Configuration FromFile(const std::string& file_path, uint32_t local_machine_id);

  uint32_t GetLocalPort() const;

private:
  Configuration(proto::Configuration&& config, uint32_t local_machine_id);

  uint32_t local_machine_id_;
  uint32_t local_replica_;
  unordered_map<uint32_t, proto::Machine> all_machines_;
  unordered_map<uint32_t, vector<uint32_t>> replica_to_machine_ids_;
};

} // namespace slog