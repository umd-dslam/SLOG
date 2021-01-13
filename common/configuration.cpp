#include "common/configuration.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/text_format.h>

#include <fstream>

#include "common/proto_utils.h"

namespace slog {

namespace {

template <class It>
uint32_t FNVHash(It begin, It end) {
  uint64_t hash = 0x811c9dc5;
  for (auto it = begin; it != end; it++) {
    hash = (hash * 0x01000193) % (1LL << 32);
    hash ^= *it;
  }
  return hash;
}

}  // namespace

using google::protobuf::io::FileInputStream;
using google::protobuf::io::ZeroCopyInputStream;
using std::string;

ConfigurationPtr Configuration::FromFile(const string& file_path, const string& local_address, uint32_t local_replica,
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

  return std::make_shared<Configuration>(config, local_address, local_replica, local_partition);
}

Configuration::Configuration(const internal::Configuration& config, const string& local_address, uint32_t local_replica,
                             uint32_t local_partition)
    : config_(config), local_address_(local_address), local_replica_(local_replica), local_partition_(local_partition) {
  for (const auto& replica : config.replicas()) {
    CHECK_EQ((uint32_t)replica.addresses_size(), config.num_partitions())
        << "Number of addresses in each replica must match number of partitions.";
    for (const auto& addr : replica.addresses()) {
      all_addresses_.push_back(addr);
    }
  }
}

const string& Configuration::protocol() const { return config_.protocol(); }

const vector<string>& Configuration::all_addresses() const { return all_addresses_; }

const string& Configuration::address(uint32_t replica, uint32_t partition) const {
  return config_.replicas(replica).addresses(partition);
}

uint32_t Configuration::num_replicas() const { return config_.replicas_size(); }

uint32_t Configuration::num_partitions() const { return config_.num_partitions(); }

uint32_t Configuration::num_workers() const { return std::max(config_.num_workers(), 1U); }

uint32_t Configuration::broker_port() const { return config_.broker_port(); }

uint32_t Configuration::server_port() const { return config_.server_port(); }

milliseconds Configuration::batch_duration() const { return milliseconds(config_.batch_duration()); }

uint32_t Configuration::replication_factor() const { return std::max(config_.replication_factor(), 1U); }

vector<MachineId> Configuration::all_machine_ids() const {
  auto num_reps = num_replicas();
  auto num_parts = num_partitions();
  vector<MachineId> ret;
  ret.reserve(num_reps * num_parts);
  for (size_t rep = 0; rep < num_reps; rep++) {
    for (size_t part = 0; part < num_parts; part++) {
      ret.push_back(MakeMachineId(rep, part));
    }
  }
  return ret;
}

const string& Configuration::local_address() const { return local_address_; }

uint32_t Configuration::local_replica() const { return local_replica_; }

uint32_t Configuration::local_partition() const { return local_partition_; }

MachineId Configuration::local_machine_id() const { return MakeMachineId(local_replica_, local_partition_); }

MachineId Configuration::MakeMachineId(uint32_t replica, uint32_t partition) const {
  return replica * num_partitions() + partition;
}

std::pair<uint32_t, uint32_t> Configuration::UnpackMachineId(MachineId machine_id) const {
  auto np = num_partitions();
  return std::make_pair(machine_id / np, machine_id % np);
}

uint32_t Configuration::leader_partition_for_multi_home_ordering() const {
  // Avoid using partition 0 here since it usually works as the
  // leader of the local paxos process
  return num_partitions() - 1;
}

uint32_t Configuration::partition_of_key(const Key& key) const {
  if (config_.has_hash_partitioning()) {
    auto end = config_.hash_partitioning().partition_key_num_bytes() >= key.length()
                   ? key.end()
                   : key.begin() + config_.hash_partitioning().partition_key_num_bytes();
    return FNVHash(key.begin(), end) % num_partitions();
  } else {
    return partition_of_key(std::stoll(key));
  }
}

bool Configuration::key_is_in_local_partition(const Key& key) const {
  return partition_of_key(key) == local_partition_;
}

uint32_t Configuration::partition_of_key(uint32_t key) const { return key % num_partitions(); }

uint32_t Configuration::master_of_key(uint32_t key) const { return (key / num_partitions()) % num_replicas(); }

const internal::SimplePartitioning* Configuration::simple_partitioning() const {
  return config_.has_simple_partitioning() ? &config_.simple_partitioning() : nullptr;
}

uint32_t Configuration::replication_delay_pct() const { return config_.replication_delay().delay_pct(); }

uint32_t Configuration::replication_delay_amount_ms() const { return config_.replication_delay().delay_amount_ms(); }

vector<TransactionEvent> Configuration::disabled_tracing_events() const {
  vector<TransactionEvent> res;
  res.reserve(config_.disabled_tracing_events_size());
  for (auto e : config_.disabled_tracing_events()) {
    res.push_back(TransactionEvent(e));
  }
  return res;
};

bool Configuration::return_dummy_txn() const { return config_.return_dummy_txn(); }

bool Configuration::bypass_mh_orderer() const { return config_.bypass_mh_orderer(); }

}  // namespace slog