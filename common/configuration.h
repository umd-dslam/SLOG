#pragma once

#include <string>
#include <unordered_map>
#include "proto/config.pb.h"
#include "proto/internal.pb.h"

using std::string;
using std::unordered_map;
using std::vector;

namespace slog {

class Configuration {
public:
  static std::shared_ptr<Configuration> FromFile(
      const string& file_path,
      const string& local_address,
      const proto::SlogIdentifier& local_identifier);

  Configuration(
      proto::Configuration&& config,
      const string& local_address,
      const proto::SlogIdentifier& local_identifier);

  uint32_t GetBrokerPort() const;
  const vector<string>& GetAllAddresses() const;
  const string& GetLocalAddress() const;
  const proto::SlogIdentifier& GetLocalSlogIdentifier() const;

private:
  uint32_t broker_port_;
  vector<string> all_addresses_;
  string local_address_;
  proto::SlogIdentifier local_identifier_;
};

} // namespace slog