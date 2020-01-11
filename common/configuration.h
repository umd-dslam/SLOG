#pragma once

#include <string>
#include <unordered_map>
#include "common/types.h"
#include "proto/configuration.pb.h"
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

  uint32_t GetGlobalPaxosMemberPartition() const;

  // TODO: How keys are partitioned is hardcoded for now. 
  //       Later find a better place to put this method
  bool KeyIsInLocalPartition(const Key& key) const;

private:
  internal::Configuration config_;
  string local_address_;
  uint32_t local_replica_;
  uint32_t local_partition_;

  vector<string> all_addresses_;
};

} // namespace slog