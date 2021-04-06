#pragma once

#include <queue>
#include <unordered_map>

#include "common/types.h"
#include "data_structure/async_log.h"
#include "proto/internal.pb.h"

namespace slog {

using BatchPtr = std::unique_ptr<internal::Batch>;

class BatchLog {
 public:
  BatchLog();

  void AddBatch(BatchPtr&& batch);
  void AddSlot(SlotId slot_id, BatchId batch_id, int replication_factor = 0);
  void AckReplication(BatchId batch_id);

  bool HasNextBatch() const;
  std::pair<SlotId, BatchPtr> NextBatch();

  /* For debugging */
  size_t NumBufferedSlots() const { return slots_.NumBufferredItems(); }

  /* For debugging */
  size_t NumBufferedBatches() const { return batches_.size(); }

 private:
  void UpdateReadyBatches();

  AsyncLog<BatchId> slots_;
  std::unordered_map<BatchId, BatchPtr> batches_;
  std::unordered_map<BatchId, int> replication_;
  std::queue<std::pair<SlotId, BatchId>> ready_batches_;
};

}  // namespace slog