#pragma once

#include "execution/tpcc/storage_adapter.h"

namespace slog {
namespace tpcc {

const int kMaxItems = 100000;
const int kDistPerWare = 10;
const int kCustPerDist = 3000;
const int kOrdPerDist = 3000;
const int kLinePerOrder = 10;

void LoadTables(const StorageAdapterPtr& storage_adapter, int W, int num_partitions, int partition,
                int num_threads = 3);

}  // namespace tpcc
}  // namespace slog