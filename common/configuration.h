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

class Configuration;

using ConfigurationPtr = std::shared_ptr<const Configuration>;

class Configuration {
public:
  static ConfigurationPtr FromFile(
      const string& file_path,
      const string& local_address = "",
      uint32_t local_replica = 0U,
      uint32_t local_partition = 0U);

  Configuration(
      const internal::Configuration& config,
      const string& local_address,
      uint32_t local_replica,
      uint32_t local_partition);

  const string& GetProtocol() const;
  const vector<string>& GetAllAddresses() const;
  const string& GetAddress(uint32_t replica, uint32_t partition) const;
  uint32_t GetBrokerPort() const;
  uint32_t GetServerPort() const;
  uint32_t GetNumReplicas() const;
  uint32_t GetNumPartitions() const;
  uint32_t GetNumWorkers() const;
  vector<string> GetAllMachineIds() const;
  long GetBatchDuration() const;

  const string& GetLocalAddress() const;
  uint32_t GetLocalReplica() const;
  uint32_t GetLocalPartition() const;
  string GetLocalMachineIdAsString() const;
  MachineId GetLocalMachineIdAsProto() const;
  uint32_t GetLocalMachineIdAsNumber() const;

  uint32_t GetLeaderPartitionForMultiHomeOrdering() const;

  // TODO: How keys are partitioned is hardcoded for now. 
  //       Maybe find a more dynamic way to do this
  uint32_t GetPartitionOfKey(const Key& key) const;
  bool KeyIsInLocalPartition(const Key& key) const;

  bool GetReplicationDelayEnabled() const;
  uint32_t GetReplicationDelayPercent() const;
  uint32_t GetReplicationDelayAmount() const;

private:
  internal::Configuration config_;
  string local_address_;
  uint32_t local_replica_;
  uint32_t local_partition_;

  vector<string> all_addresses_;
};

} // namespace slog