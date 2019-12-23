#include "common/configuration.h"

#include <fstream>
#include <sstream>
#include <stdexcept>

#include <google/protobuf/text_format.h>
#include <glog/logging.h>

namespace slog {

using std::runtime_error;

std::shared_ptr<Configuration> Configuration::FromFile(
    const std::string& file_path, 
    const std::string& local_address,
    const SlogIdentifier& local_id) {
  std::ifstream ifs(file_path);
  CHECK(ifs.is_open()) << "Configuration file not found";

  std::stringstream ss;
  ss << ifs.rdbuf();

  internal::Configuration config;
  std::string str = ss.str();
  google::protobuf::TextFormat::ParseFromString(str, &config);

  return std::make_shared<Configuration>(config, local_address, local_id);
}

Configuration::Configuration(
    const internal::Configuration& config, 
    const std::string& local_address,
    const SlogIdentifier& local_id) 
  : protocol_(config.protocol()),
    broker_port_(config.broker_port()),
    server_port_(config.server_port()),
    num_replicas_(config.num_replicas()),
    num_partitions_(config.num_partitions()),
    local_address_(local_address),
    local_id_(local_id) {

  for (int i = 0; i < config.addresses_size(); i++) {
    all_addresses_.push_back(config.addresses(i));
  }
  CHECK(std::find(
      all_addresses_.begin(), 
      all_addresses_.end(), 
      local_address) != all_addresses_.end()) 
      << "Local machine ID is not present in the configuration";
  }

const string& Configuration::GetProtocol() const {
  return protocol_;
}

const vector<string>& Configuration::GetAllAddresses() const {
  return all_addresses_;
}

uint32_t Configuration::GetNumReplicas() const {
  return num_replicas_;
}

uint32_t Configuration::GetNumPartitions() const {
  return num_partitions_;
}

uint32_t Configuration::GetBrokerPort() const {
  return broker_port_;
}

uint32_t Configuration::GetServerPort() const {
  return server_port_;
}

const string& Configuration::GetLocalAddress() const {
  return local_address_;
}

const SlogIdentifier& Configuration::GetLocalSlogId() const {
  return local_id_;
}

uint32_t Configuration::GetLocalNumericId() const {
  return local_id_.partition() * 100 + local_id_.replica();
}

} // namespace slog