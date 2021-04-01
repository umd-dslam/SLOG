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
  static ConfigurationPtr FromFile(const string& file_path, const string& local_address = "");

  Configuration(const internal::Configuration& config, const string& local_address);

  const string& protocol() const;
  const vector<string>& all_addresses() const;
  const string& address(uint32_t replica, uint32_t partition) const;
  const string& address(MachineId machine_id) const;
  uint32_t broker_ports(int i) const;
  uint32_t broker_ports_size() const;
  uint32_t server_port() const;
  uint32_t num_replicas() const;
  uint32_t num_partitions() const;
  uint32_t num_workers() const;
  vector<MachineId> all_machine_ids() const;
  milliseconds forwarder_batch_duration() const;
  int forwarder_max_batch_size() const;
  milliseconds sequencer_batch_duration() const;
  int sequencer_max_batch_size() const;
  uint32_t replication_factor() const;

  const string& local_address() const;
  uint32_t local_replica() const;
  uint32_t local_partition() const;
  MachineId local_machine_id() const;
  MachineId MakeMachineId(uint32_t replica, uint32_t partition) const;
  std::pair<uint32_t, uint32_t> UnpackMachineId(MachineId machine_id) const;

  uint32_t leader_replica_for_multi_home_ordering() const;
  uint32_t leader_partition_for_multi_home_ordering() const;

  uint32_t partition_of_key(const Key& key) const;
  bool key_is_in_local_partition(const Key& key) const;
  // Only work with simple partitioning
  int partition_of_key(uint32_t key) const;
  uint32_t master_of_key(uint32_t key) const;
  const internal::SimplePartitioning* simple_partitioning() const;

  uint32_t replication_delay_pct() const;
  uint32_t replication_delay_amount_ms() const;

  vector<TransactionEvent> disabled_tracing_events() const;
  bool bypass_mh_orderer() const;
  vector<int> cpu_pinnings(ModuleId module) const;
  bool return_dummy_txn() const;
  int recv_retries() const;
  internal::Commands commands() const;
  uint32_t latency(size_t i) const;
  std::pair<uint32_t, size_t> nth_latency(size_t n) const;
  bool synchronized_batching() const;

 private:
  internal::Configuration config_;
  string local_address_;
  int local_replica_;
  int local_partition_;

  vector<string> all_addresses_;
  vector<uint32_t> latency_;
  vector<std::pair<uint32_t, size_t>> ordered_latency_;
};

}  // namespace slog