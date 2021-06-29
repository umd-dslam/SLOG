#pragma once

#include "common/configuration.h"
#include "common/types.h"

namespace slog {

class Sharder;

using SharderPtr = std::shared_ptr<Sharder>;

class Sharder {
 public:
  static std::shared_ptr<Sharder> MakeSharder(const ConfigurationPtr& config);

  Sharder(const ConfigurationPtr& config);

  bool is_local_key(const Key& key) const;
  uint32_t num_partitions() const;
  uint32_t local_partition() const;

  virtual uint32_t compute_partition(const Key& key) const = 0;

 protected:
  uint32_t local_partition_;
  uint32_t num_partitions_;
};

class HashSharder : public Sharder {
 public:
  HashSharder(const ConfigurationPtr& config);
  uint32_t compute_partition(const Key& key) const final;

 private:
  size_t partition_key_num_bytes_;
};

class SimpleSharder : public Sharder {
 public:
  SimpleSharder(const ConfigurationPtr& config);
  uint32_t compute_partition(const Key& key) const final;
};

class TPCCSharder : public Sharder {
 public:
  TPCCSharder(const ConfigurationPtr& config);
  uint32_t compute_partition(const Key& key) const final;
};

}  // namespace slog