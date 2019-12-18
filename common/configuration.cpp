#include "configuration.h"

#include <fstream>
#include <sstream>
#include <stdexcept>

#include <google/protobuf/text_format.h>
#include <glog/logging.h>

namespace slog {

using std::runtime_error;

std::shared_ptr<Configuration> Configuration::FromFile(const std::string& file_path, uint32_t local_machine_id) {
  std::ifstream ifs(file_path);
  CHECK(ifs.is_open()) << "Configuration file not found";

  std::stringstream ss;
  ss << ifs.rdbuf();

  proto::Configuration config;
  std::string str = ss.str();
  google::protobuf::TextFormat::ParseFromString(str, &config);

  return std::make_shared<Configuration>(std::move(config), local_machine_id);
}

Configuration::Configuration(
    proto::Configuration&& config, 
    uint32_t local_machine_id) 
  : local_machine_id_(local_machine_id),
    broker_port_(config.broker_port()) {

  for (int i = 0; i < config.machines_size(); i++) {
    const auto& machine = config.machines(i);
    uint32_t machine_id = machine.id();
    all_machines_[machine_id] = machine;
    replica_to_machine_ids_[machine.replica()].push_back(machine_id);
  }
  CHECK(all_machines_.count(local_machine_id) > 0) 
      << "Local machine ID is not present in the configuration";
}

uint32_t Configuration::GetBrokerPort() const {
  return broker_port_;
}

} // namespace slog