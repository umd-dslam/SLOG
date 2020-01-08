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
  : protocol_(config.protocol()),
    broker_port_(config.broker_port()),
    server_port_(config.server_port()),
    num_replicas_(config.num_replicas()),
    num_partitions_(config.num_partitions()),
    partition_key_num_bytes_(config.partition_key_num_bytes()),
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
  return protocol_;
}

const vector<string>& Configuration::GetAllAddresses() const {
  return all_addresses_;
}

uint32_t Configuration::GetNumReplicas() const {
  return num_replicas_;
}

uint32_t Configuration::GetNumPartitions() const {
  return num_partitions_;
}

uint32_t Configuration::GetBrokerPort() const {
  return broker_port_;
}

uint32_t Configuration::GetServerPort() const {
  return server_port_;
}

vector<string> Configuration::GetAllMachineIds() const {
  vector<string> ret;
  ret.reserve(num_replicas_ * num_partitions_);
  for (size_t rep = 0; rep < num_replicas_; rep++) {
    for (size_t part = 0; part < num_partitions_; part++) {
      ret.push_back(MakeMachineId(rep, part));
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
  return MakeMachineIdProto(local_replica_, local_partition_);
}

uint32_t Configuration::GetLocalMachineIdAsNumber() const {
  return local_replica_ * num_partitions_ + local_partition_;
}

string Configuration::GetLocalMachineIdAsString() const {
  return MakeMachineId(local_replica_, local_partition_);
}

uint32_t Configuration::GetGlobalPaxosMemberPartition() const {
  // This can be any partition other than partition 0 because
  // partition 0 is probably busy working as the leader of the 
  // local paxos process
  return num_partitions_ - 1;
}

bool Configuration::KeyIsInLocalPartition(const Key& key) const {
  auto end = partition_key_num_bytes_ >= key.length() 
      ? key.end() 
      : key.begin() + partition_key_num_bytes_;
  auto owning_partition = 
      FNVHash(key.begin(), end) % num_partitions_;
  return owning_partition == local_partition_;
}

} // namespace slog