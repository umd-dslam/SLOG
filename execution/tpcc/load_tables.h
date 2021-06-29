#pragma once

#include "execution/tpcc/constants.h"
#include "execution/tpcc/storage_adapter.h"

namespace slog {
namespace tpcc {

void LoadTables(const StorageAdapterPtr& storage_adapter, int W, int num_replicas, int num_partitions, int partition,
                int num_threads = 3);

}  // namespace tpcc
}  // namespace slog