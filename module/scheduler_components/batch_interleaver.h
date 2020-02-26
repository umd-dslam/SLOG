#pragma once

#include <queue>
#include <unordered_map>

#include "common/async_log.h"
#include "common/types.h"
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

private:
  void UpdateReadyBatches();

  AsyncLog<uint32_t> slots_;
  unordered_map<uint32_t, AsyncLog<BatchId>> batch_queues_;
  queue<pair<SlotId, BatchId>> ready_batches_;
};

} // namespace slog