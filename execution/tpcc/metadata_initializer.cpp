#include "execution/tpcc/metadata_initializer.h"

#include <glog/logging.h>

namespace slog {
namespace tpcc {

TPCCMetadataInitializer::TPCCMetadataInitializer(uint32_t num_replicas, uint32_t num_partitions)
    : num_replicas_(num_replicas), num_partitions_(num_partitions) {}

Metadata TPCCMetadataInitializer::Compute(const Key& key) {
  CHECK_GE(key.size(), 4) << "Invalid key";
  uint32_t warehouse_id = *reinterpret_cast<const uint32_t*>(key.data());
  return Metadata(((warehouse_id - 1) / num_partitions_) % num_replicas_);
}

}  // namespace tpcc
}  // namespace slog