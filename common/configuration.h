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
  vector<MachineIdNum> GetAllMachineIds() const;
  long GetBatchDuration() const;

  const string& GetLocalAddress() const;
  uint32_t GetLocalReplica() const;
  uint32_t GetLocalPartition() const;
  string GetLocalMachineIdAsString() const;
  internal::MachineId GetLocalMachineIdAsProto() const;
  uint32_t GetLocalMachineIdAsNumber() const;
  MachineIdNum MakeMachineIdNum(uint32_t replica, uint32_t partition) const;
  std::pair<uint32_t, uint32_t> UnpackMachineId(MachineIdNum machine_id) const;

  uint32_t GetLeaderPartitionForMultiHomeOrdering() const;

  uint32_t GetPartitionOfKey(const Key& key) const;
  bool KeyIsInLocalPartition(const Key& key) const;
  // Only work with simple partitioning
  uint32_t GetPartitionOfKey(uint32_t key) const;
  uint32_t GetMasterOfKey(uint32_t key) const;
  const internal::SimplePartitioning* GetSimplePartitioning() const;

#ifdef ENABLE_REPLICATION_DELAY
  uint32_t GetReplicationDelayPercent() const;
  uint32_t GetReplicationDelayAmount() const;
#endif /* ENABLE_REPLICATION_DELAY */

private:
  internal::Configuration config_;
  string local_address_;
  uint32_t local_replica_;
  uint32_t local_partition_;

  vector<string> all_addresses_;
};

} // namespace slog