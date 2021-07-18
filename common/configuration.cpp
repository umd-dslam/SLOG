#include "common/configuration.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/text_format.h>

#include <fstream>

#include "common/constants.h"
#include "common/proto_utils.h"
#include "common/string_utils.h"

namespace slog {

using google::protobuf::io::FileInputStream;
using google::protobuf::io::ZeroCopyInputStream;
using std::chrono::milliseconds;
using std::chrono::operator""ms;
using std::string;
using std::vector;

ConfigurationPtr Configuration::FromFile(const string& file_path, const string& local_address) {
  int fd = open(file_path.c_str(), O_RDONLY);
  if (fd < 0) {
    LOG(FATAL) << "Configuration file error: " << strerror(errno);
  }
  ZeroCopyInputStream* input = new FileInputStream(fd);
  internal::Configuration config;

  google::protobuf::TextFormat::Parse(input, &config);

  delete input;
  close(fd);

  return std::make_shared<Configuration>(config, local_address);
}

Configuration::Configuration(const internal::Configuration& config, const string& local_address)
    : config_(config), local_address_(local_address), local_replica_(0), local_partition_(0) {
  CHECK_LE(config_.replication_factor(), config_.replicas_size())
      << "Replication factor must not exceed number of replicas";
  CHECK_LE(config_.broker_ports_size(), kMaxChannel - kBrokerChannel)
      << "Maximum number of broker threads is " << kMaxChannel - kBrokerChannel;
  CHECK_GT(config_.broker_ports_size(), 0) << "There must be at least one broker";
  CHECK_NE(config_.server_port(), 0) << "Server port must be set";
  CHECK_NE(config_.sequencer_port(), 0) << "Sequencer port must be set";
  CHECK_NE(config_.forwarder_port(), 0) << "Forwarder port must be set";

  bool local_address_is_valid = local_address_.empty();
  for (int r = 0; r < config_.replicas_size(); r++) {
    auto& replica = config_.replicas(r);
    CHECK_EQ((uint32_t)replica.addresses_size(), config_.num_partitions())
        << "Number of addresses in each replica must match number of partitions.";
    for (int p = 0; p < replica.addresses_size(); p++) {
      auto& address = replica.addresses(p);
      all_addresses_.push_back(address);
      if (address == local_address) {
        local_address_is_valid = true;
        local_replica_ = r;
        local_partition_ = p;
      }
    }
  }

  CHECK(local_address_is_valid) << "The configuration does not contain the provided local machine ID: \""
                                << local_address_ << "\"";

  if (config_.execution_type() == internal::ExecutionType::TPC_C) {
    CHECK(config_.has_tpcc_partitioning()) << "TPC-C execution type can only be paired with TPC-C partitioning";
  }

  if (config_.has_tpcc_partitioning()) {
    CHECK(config_.execution_type() == internal::ExecutionType::TPC_C)
        << "TPC-C partitioning can only be paired with TPC-C execution type";
  }

  if (config_.replication_order_size() > local_replica_) {
    auto order_str = Split(config_.replication_order(local_replica_), ",");
    for (auto rstr : order_str) {
      auto r = std::stol(rstr);
      CHECK_LT(r, config_.replicas_size()) << "Invalid replica specified in the replication order";
      if (r != local_replica_) {
        replication_order_.push_back(r);
      }
    }
  }
}

const internal::Configuration& Configuration::proto_config() const { return config_; }

const string& Configuration::protocol() const { return config_.protocol(); }

const vector<string>& Configuration::all_addresses() const { return all_addresses_; }

const string& Configuration::address(uint32_t replica, uint32_t partition) const {
  return config_.replicas(replica).addresses(partition);
}

const string& Configuration::address(MachineId machine_id) const { return all_addresses_[machine_id]; }

uint32_t Configuration::num_replicas() const { return config_.replicas_size(); }

uint32_t Configuration::num_partitions() const { return config_.num_partitions(); }

uint32_t Configuration::num_workers() const { return std::max(config_.num_workers(), 1U); }

uint32_t Configuration::broker_ports(int i) const { return config_.broker_ports(i); }
uint32_t Configuration::broker_ports_size() const { return config_.broker_ports_size(); }

uint32_t Configuration::server_port() const { return config_.server_port(); }

uint32_t Configuration::forwarder_port() const { return config_.forwarder_port(); }

uint32_t Configuration::sequencer_port() const { return config_.sequencer_port(); }

milliseconds Configuration::forwarder_batch_duration() const {
  return milliseconds(config_.forwarder_batch_duration());
}

milliseconds Configuration::sequencer_batch_duration() const {
  if (config_.sequencer_batch_duration() == 0) {
    return 1ms;
  }
  return milliseconds(config_.sequencer_batch_duration());
}

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

uint32_t Configuration::leader_replica_for_multi_home_ordering() const { return 0; }

uint32_t Configuration::leader_partition_for_multi_home_ordering() const {
  // Avoid using partition 0 here since that partition already works as the
  // leader of the local paxos process
  return num_partitions() - 1;
}

uint32_t Configuration::replication_delay_pct() const { return config_.replication_delay().delay_pct(); }

uint32_t Configuration::replication_delay_amount_ms() const { return config_.replication_delay().delay_amount_ms(); }

vector<TransactionEvent> Configuration::enabled_events() const {
  vector<TransactionEvent> res;
  res.reserve(config_.enabled_events_size());
  for (auto e : config_.enabled_events()) {
    res.push_back(TransactionEvent(e));
  }
  return res;
};

bool Configuration::bypass_mh_orderer() const { return config_.bypass_mh_orderer(); }

vector<int> Configuration::cpu_pinnings(ModuleId module) const {
  vector<int> cpus;
  for (auto& entry : config_.cpu_pinnings()) {
    if (entry.module() == module) {
      cpus.push_back(entry.cpu());
    }
  }
  return cpus;
}

bool Configuration::return_dummy_txn() const { return config_.return_dummy_txn(); }

int Configuration::recv_retries() const { return config_.recv_retries() == 0 ? 1000 : config_.recv_retries(); }

internal::ExecutionType Configuration::execution_type() const { return config_.execution_type(); }

const vector<uint32_t> Configuration::replication_order() const { return replication_order_; }

bool Configuration::synchronized_batching() const { return config_.synchronized_batching(); }

uint32_t Configuration::sample_rate() const { return config_.sample_rate(); }

std::vector<int> Configuration::distance_ranking_from(int replica_id) const {
  auto ranking_str = Split(config_.replicas(replica_id).distance_ranking(), ",");
  std::vector<int> ranking;
  for (auto s : ranking_str) {
    ranking.push_back(std::stoi(s));
  }
  return ranking;
}

}  // namespace slog