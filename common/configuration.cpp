#include "common/configuration.h"

#include <fcntl.h>
#include <fstream>

#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/text_format.h>
#include <glog/logging.h>

#include "common/proto_utils.h"

namespace slog {

namespace {

template<class It>
uint32_t FNVHash(It begin, It end) {
  uint64_t hash = 0x811c9dc5;
  for (auto it = begin; it != end; it++) {
    hash = (hash * 0x01000193) % (1LL << 32);
    hash ^= *it;
  }
  return hash;
}

} // namespace

using std::string;
using google::protobuf::io::ZeroCopyInputStream;
using google::protobuf::io::FileInputStream;

ConfigurationPtr Configuration::FromFile(
    const string& file_path, 
    const string& local_address,
    uint32_t local_replica,
    uint32_t local_partition) {
  int fd = open(file_path.c_str(), O_RDONLY);
  if (fd < 0) {
    LOG(FATAL) << "Configuration file error: " << strerror(errno);
  }
  ZeroCopyInputStream* input = new FileInputStream(fd);
  internal::Configuration config;

  google::protobuf::TextFormat::Parse(input, &config);

  delete input;
  close(fd);

  return std::make_shared<Configuration>(
      config, local_address, local_replica, local_partition);
}

Configuration::Configuration(
    const internal::Configuration& config,
    const string& local_address,
    uint32_t local_replica,
    uint32_t local_partition)
  : config_(config),
    local_address_(local_address),
    local_replica_(local_replica),
    local_partition_(local_partition) {
  for (const auto& replica : config.replicas()) {
    CHECK_EQ((uint32_t)replica.addresses_size(), config.num_partitions())
        << "Number of addresses in each replica must match number of partitions.";
    for (const auto& addr : replica.addresses()) {
      all_addresses_.push_back(addr);
    }
  }
}

const string& Configuration::GetProtocol() const {
  return config_.protocol();
}

const vector<string>& Configuration::GetAllAddresses() const {
  return all_addresses_;
}

const string& Configuration::GetAddress(uint32_t replica, uint32_t partition) const {
  return config_.replicas(replica).addresses(partition);
}

uint32_t Configuration::GetNumReplicas() const {
  return config_.replicas_size();
}

uint32_t Configuration::GetNumPartitions() const {
  return config_.num_partitions();
}

uint32_t Configuration::GetNumWorkers() const {
  return std::max(config_.num_workers(), 1U);
}

uint32_t Configuration::GetBrokerPort() const {
  return config_.broker_port();
}

uint32_t Configuration::GetServerPort() const {
  return config_.server_port();
}

long Configuration::GetBatchDuration() const {
  return config_.batch_duration();
}

vector<string> Configuration::GetAllMachineIds() const {
  auto num_reps = GetNumReplicas();
  auto num_parts = GetNumPartitions();
  vector<string> ret;
  ret.reserve(num_reps * num_parts);
  for (size_t rep = 0; rep < num_reps; rep++) {
    for (size_t part = 0; part < num_parts; part++) {
      ret.push_back(MakeMachineIdAsString(rep, part));
    }
  }
  return ret;
}

const string& Configuration::GetLocalAddress() const {
  return local_address_;
}

uint32_t Configuration::GetLocalReplica() const {
  return local_replica_;
}

uint32_t Configuration::GetLocalPartition() const {
  return local_partition_;
}

MachineId Configuration::GetLocalMachineIdAsProto() const {
  return MakeMachineId(local_replica_, local_partition_);
}

uint32_t Configuration::GetLocalMachineIdAsNumber() const {
  return local_replica_ * GetNumPartitions() + local_partition_;
}

string Configuration::GetLocalMachineIdAsString() const {
  return MakeMachineIdAsString(local_replica_, local_partition_);
}

uint32_t Configuration::GetLeaderPartitionForMultiHomeOrdering() const {
  // Avoid using partition 0 here since it usually works as the
  // leader of the local paxos process
  return GetNumPartitions() - 1;
}

uint32_t Configuration::GetPartitionOfKey(const Key& key) const {
  auto end = config_.partition_key_num_bytes() >= key.length() 
      ? key.end() 
      : key.begin() + config_.partition_key_num_bytes();
  return FNVHash(key.begin(), end) % GetNumPartitions();
}

bool Configuration::KeyIsInLocalPartition(const Key& key) const {
  return GetPartitionOfKey(key) == local_partition_;
}

bool Configuration::GetReplicationDelayEnabled() const {
  return config_.replication_delay().batch_delay_percent() != 0;
}

uint32_t Configuration::GetReplicationDelayPercent() const {
  return config_.replication_delay().batch_delay_percent();
}

uint32_t Configuration::GetReplicationDelayAmount() const {
  return config_.replication_delay().batch_delay_amount();
}


} // namespace slog