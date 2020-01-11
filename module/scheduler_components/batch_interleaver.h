#pragma once

#include <deque>
#include <unordered_map>

#include "common/types.h"
#include "proto/internal.pb.h"

using std::deque;
using std::shared_ptr;
using std::pair;
using std::unordered_map;

namespace slog {

using BatchPtr = shared_ptr<internal::Batch>;

class BatchInterleaver {
public:
  BatchInterleaver();

  void AddBatch(uint32_t queue_id, BatchPtr batch);
  void AddAgreedSlot(SlotId slot_id, uint32_t queue_id);

  bool HasReadyBatch() const;
  pair<BatchPtr, SlotId> NextBatch();

private:
  void UpdateReadyBatches();

  unordered_map<uint32_t, deque<BatchPtr>> batch_queues_;
  unordered_map<SlotId, uint32_t> pending_slots_;
  SlotId next_slot_;
  deque<pair<BatchPtr, SlotId>> ready_batches_;
};

} // namespace slog