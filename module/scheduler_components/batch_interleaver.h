#pragma once

#include <queue>
#include <unordered_map>

#include "common/types.h"
#include "data_structure/async_log.h"
#include "proto/internal.pb.h"

using std::queue;
using std::pair;
using std::unordered_map;

namespace slog {

/**
 * A BatchInterleaver interleaves batches from different batch queues.
 * The order for which queue to dequeue from is given in the manner
 * similar to an asynchronous log.
 */
class BatchInterleaver {
public:
  BatchInterleaver();

  void AddBatchId(
      uint32_t queue_id, uint32_t position, BatchId batch_id);
  void AddSlot(SlotId slot_id, uint32_t queue_id);

  bool HasNextBatch() const;
  pair<SlotId, BatchId> NextBatch();

  /* For debugging */
  size_t NumBufferedSlots() const {
    return slots_.NumBufferredItems();
  }

  /* For debugging */
  unordered_map<uint32_t, size_t> NumBufferedBatchesPerQueue() const {
    unordered_map<uint32_t, size_t> queue_sizes;
    for (const auto& pair : batch_queues_) {
      queue_sizes[pair.first] = pair.second.NumBufferredItems();
    }
    return queue_sizes;
  }

private:
  void UpdateReadyBatches();

  AsyncLog<uint32_t> slots_;
  unordered_map<uint32_t, AsyncLog<BatchId>> batch_queues_;
  queue<pair<SlotId, BatchId>> ready_batches_;
};

} // namespace slog