#include "common/sharder.h"

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

std::shared_ptr<Sharder> Sharder::MakeSharder(const ConfigurationPtr& config) {
  if (config->proto_config().has_simple_partitioning()) {
    return std::make_shared<SimpleSharder>(config);
  } else if (config->proto_config().has_tpcc_partitioning()) {
    return std::make_shared<TPCCSharder>(config);
  }
  return std::make_shared<HashSharder>(config);
}

Sharder::Sharder(const ConfigurationPtr& config)
    : local_partition_(config->local_partition()), num_partitions_(config->num_partitions()) {}

bool Sharder::is_local_key(const Key& key) const { return compute_partition(key) == local_partition_; }

uint32_t Sharder::num_partitions() const { return num_partitions_; }

uint32_t Sharder::local_partition() const { return local_partition_; }

HashSharder::HashSharder(const ConfigurationPtr& config)
    : Sharder(config), partition_key_num_bytes_(config->proto_config().hash_partitioning().partition_key_num_bytes()) {}

uint32_t HashSharder::compute_partition(const Key& key) const {
  auto end = partition_key_num_bytes_ >= key.length() ? key.end() : key.begin() + partition_key_num_bytes_;
  return FNVHash(key.begin(), end) % num_partitions_;
}

SimpleSharder::SimpleSharder(const ConfigurationPtr& config) : Sharder(config) {}

uint32_t SimpleSharder::compute_partition(const Key& key) const { return std::stoll(key) % num_partitions_; }

TPCCSharder::TPCCSharder(const ConfigurationPtr& config) : Sharder(config) {}
uint32_t TPCCSharder::compute_partition(const Key& key) const {
  int w_id = *reinterpret_cast<const int*>(key.data());
  return (w_id - 1) % num_partitions_;
}

}  // namespace slog