#pragma once

#include <memory>
#include <unordered_map>

#include "common/types.h"
#include "proto/internal.pb.h"

using std::shared_ptr;
using std::unordered_map;

namespace slog {

using BatchPtr = shared_ptr<internal::Batch>;

class LocalLog {
public:
  LocalLog();
  void AddBatch(SlotId slot_id, BatchPtr batch);
  bool HasNextBatch() const;
  BatchPtr NextBatch();

private:
  unordered_map<SlotId, BatchPtr> batches_;
  SlotId next_slot_;
};

} // namespace slog