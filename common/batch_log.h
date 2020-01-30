#pragma once

#include <queue>
#include <unordered_map>

#include "common/async_log.h"
#include "common/types.h"
#include "proto/internal.pb.h"

namespace slog {

using BatchPtr = std::unique_ptr<internal::Batch>;

class BatchLog {
public:
  BatchLog();

  void AddBatch(BatchPtr&& batch);
  void AddSlot(SlotId slot_id, BatchId batch_id);

  bool HasNextBatch() const;
  std::pair<SlotId, BatchPtr> NextBatch();

private:
  void UpdateReadyBatches();

  AsyncLog<BatchId> slots_;
  std::unordered_map<BatchId, BatchPtr> batches_;
  std::queue<std::pair<SlotId, BatchId>> ready_batches_;
};

} // namespace slog