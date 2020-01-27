#pragma once

#include <queue>
#include <unordered_map>

#include "common/types.h"
#include "proto/internal.pb.h"

using std::queue;
using std::pair;
using std::unordered_map;

namespace slog {

class BatchInterleaver {
public:
  BatchInterleaver();

  void AddBatchId(uint32_t queue_id, BatchId batch_id);
  void AddSlot(SlotId slot_id, uint32_t queue_id);

  bool HasNextBatch() const;
  pair<SlotId, BatchId> NextBatch();

private:
  void UpdateReadyBatches();

  unordered_map<SlotId, uint32_t> pending_slots_;
  unordered_map<uint32_t, queue<BatchId>> batch_queues_;
  SlotId next_slot_;
  queue<pair<SlotId, BatchId>> ready_batches_;
};

} // namespace slog