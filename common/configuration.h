#pragma once

#include <string>

#include "common/types.h"
#include "proto/configuration.pb.h"
#include "proto/internal.pb.h"

using std::string;
using std::vector;

namespace slog {

class Configuration;

using ConfigurationPtr = std::shared_ptr<const Configuration>;

class Configuration {
 public:
  static ConfigurationPtr FromFile(const string& file_path, const string& local_address = "",
                                   uint32_t local_replica = 0U, uint32_t local_partition = 0U);

  Configuration(const internal::Configuration& config, const string& local_address, uint32_t local_replica,
                uint32_t local_partition);

  const string& protocol() const;
  const vector<string>& all_addresses() const;
  const string& address(uint32_t replica, uint32_t partition) const;
  uint32_t broker_port() const;
  uint32_t server_port() const;
  uint32_t num_replicas() const;
  uint32_t num_partitions() const;
  uint32_t num_workers() const;
  vector<MachineId> all_machine_ids() const;
  long batch_duration() const;
  uint32_t replication_factor() const;

  const string& local_address() const;
  uint32_t local_replica() const;
  uint32_t local_partition() const;
  uint32_t local_machine_id() const;
  MachineId MakeMachineId(uint32_t replica, uint32_t partition) const;
  std::pair<uint32_t, uint32_t> UnpackMachineId(MachineId machine_id) const;

  uint32_t leader_partition_for_multi_home_ordering() const;

  uint32_t partition_of_key(const Key& key) const;
  bool key_is_in_local_partition(const Key& key) const;
  // Only work with simple partitioning
  uint32_t partition_of_key(uint32_t key) const;
  uint32_t master_of_key(uint32_t key) const;
  const internal::SimplePartitioning* simple_partitioning() const;

  uint32_t replication_delay_percent() const;
  uint32_t replication_delay_amount() const;

 private:
  internal::Configuration config_;
  string local_address_;
  uint32_t local_replica_;
  uint32_t local_partition_;

  vector<string> all_addresses_;
};

}  // namespace slog