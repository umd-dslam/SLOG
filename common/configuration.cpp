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
    const proto::SlogIdentifier& local_identifier) {
  std::ifstream ifs(file_path);
  CHECK(ifs.is_open()) << "Configuration file not found";

  std::stringstream ss;
  ss << ifs.rdbuf();

  proto::Configuration config;
  std::string str = ss.str();
  google::protobuf::TextFormat::ParseFromString(str, &config);

  return std::make_shared<Configuration>(std::move(config), local_address, local_identifier);
}

Configuration::Configuration(
    proto::Configuration&& config, 
    const std::string& local_address,
    const proto::SlogIdentifier& local_identifier) 
  : broker_port_(config.broker_port()),
    local_address_(local_address),
    local_identifier_(local_identifier) {

  for (int i = 0; i < config.addresses_size(); i++) {
    all_addresses_.push_back(config.addresses(i));
  }
  CHECK(std::find(
      all_addresses_.begin(), 
      all_addresses_.end(), 
      local_address) != all_addresses_.end()) 
      << "Local machine ID is not present in the configuration";
}

uint32_t Configuration::GetBrokerPort() const {
  return broker_port_;
}

const vector<string>& Configuration::GetAllAddresses() const {
  return all_addresses_;
}

const string& Configuration::GetLocalAddress() const {
  return local_address_;
}
const proto::SlogIdentifier& Configuration::GetLocalSlogIdentifier() const {
  return local_identifier_;
}

} // namespace slog