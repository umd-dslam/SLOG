#pragma once

#include <string>
#include <unordered_map>
#include "proto/internal.pb.h"

using std::string;
using std::unordered_map;
using std::vector;

namespace slog {

using internal::MachineId;

class Configuration {
public:
  static std::shared_ptr<Configuration> FromFile(
      const string& file_path,
      const string& local_address,
      uint32_t local_replica,
      uint32_t local_partition);

  Configuration(
      const internal::Configuration& config,
      const string& local_address,
      uint32_t local_replica,
      uint32_t local_partition);

  const string& GetProtocol() const;
  const vector<string>& GetAllAddresses() const;
  uint32_t GetBrokerPort() const;
  uint32_t GetServerPort() const;
  uint32_t GetNumReplicas() const;
  uint32_t GetNumPartitions() const;
  vector<string> GetAllMachineIds() const;

  const string& GetLocalAddress() const;
  uint32_t GetLocalReplica() const;
  uint32_t GetLocalPartition() const;
  string GetLocalMachineIdAsString() const;
  MachineId GetLocalMachineIdAsProto() const;
  uint32_t GetLocalMachineIdAsNumber() const;

private:
  string protocol_;
  uint32_t broker_port_;
  uint32_t server_port_;
  uint32_t num_replicas_;
  uint32_t num_partitions_;
  vector<string> all_addresses_;
  string local_address_;
  uint32_t local_replica_;
  uint32_t local_partition_;
};

} // namespace slog