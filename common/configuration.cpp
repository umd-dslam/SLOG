#include "common/configuration.h"

#include <fstream>
#include <sstream>
#include <stdexcept>

#include <google/protobuf/text_format.h>
#include <glog/logging.h>

#include "common/proto_utils.h"

namespace slog {

namespace {

template<class It>
uint32_t FNVHash(It begin, It end) {
  uint32_t hash = 2166136261;
  for (auto it = begin; it != end; it++) {
    hash = (hash * 1099511628211) ^ *it;
  }
  return hash;
}

} // namespace

using std::runtime_error;

std::shared_ptr<Configuration> Configuration::FromFile(
    const std::string& file_path, 
    const std::string& local_address,
    uint32_t local_replica,
    uint32_t local_partition) {
  std::ifstream ifs(file_path);
  CHECK(ifs.is_open()) << "Configuration file not found";

  std::stringstream ss;
  ss << ifs.rdbuf();

  internal::Configuration config;
  std::string str = ss.str();
  google::protobuf::TextFormat::ParseFromString(str, &config);

  return std::make_shared<Configuration>(
      config, local_address, local_replica, local_partition);
}

Configuration::Configuration(
    const internal::Configuration& config,
    const std::string& local_address,
    uint32_t local_replica,
    uint32_t local_partition)
  : config_(config),
    local_address_(local_address),
    local_replica_(local_replica),
    local_partition_(local_partition) {

  for (int i = 0; i < config.addresses_size(); i++) {
    all_addresses_.push_back(config.addresses(i));
  }
  CHECK(std::find(
      all_addresses_.begin(),
      all_addresses_.end(),
      local_address) != all_addresses_.end())
      << "The configuration does not contain the provided "
      << "local machine ID: \"" << local_address << "\"";
}

const string& Configuration::GetProtocol() const {
  return config_.protocol();
}

const vector<string>& Configuration::GetAllAddresses() const {
  return all_addresses_;
}

uint32_t Configuration::GetNumReplicas() const {
  return config_.num_replicas();
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

uint32_t Configuration::KeyToPartition(const Key& key) const {
  auto end = config_.partition_key_num_bytes() >= key.length() 
      ? key.end() 
      : key.begin() + config_.partition_key_num_bytes();
  return FNVHash(key.begin(), end) % GetNumPartitions();
}

bool Configuration::KeyIsInLocalPartition(const Key& key) const {
  return KeyToPartition(key) == local_partition_;
}

} // namespace slog