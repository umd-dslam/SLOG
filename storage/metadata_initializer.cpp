#include "storage/metadata_initializer.h"

namespace slog {

SimpleMetadataInitializer::SimpleMetadataInitializer(uint32_t num_replicas, uint32_t num_partitions)
    : num_replicas_(num_replicas), num_partitions_(num_partitions) {}

Metadata SimpleMetadataInitializer::Compute(const Key& key) {
  return Metadata((std::stoull(key) / num_partitions_) % num_replicas_);
}

ConstantMetadataInitializer::ConstantMetadataInitializer(uint32_t home) : home_(home) {}

Metadata ConstantMetadataInitializer::Compute(const Key&) { return Metadata(home_); }

}  // namespace slog