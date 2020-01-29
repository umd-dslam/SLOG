#include "batch_interleaver.h"

#include <glog/logging.h>

#include "common/constants.h"

namespace slog {

BatchInterleaver::BatchInterleaver() {}

void BatchInterleaver::AddBatchId(
    uint32_t queue_id, uint32_t position, BatchId batch_id) {
  batch_queues_[queue_id].Insert(position, batch_id);
  UpdateReadyBatches();
}

void BatchInterleaver::AddSlot(SlotId slot_id, uint32_t queue_id) {
  slots_.Insert(slot_id, queue_id);
  UpdateReadyBatches();
}

bool BatchInterleaver::HasNextBatch() const {
  return !ready_batches_.empty();
}

pair<SlotId, BatchId> BatchInterleaver::NextBatch() {
  if (!HasNextBatch()) {
    throw std::runtime_error("NextBatch() was called when there is no batch");
  }
  auto next_batch = ready_batches_.front();
  ready_batches_.pop();
  return next_batch;
}

void BatchInterleaver::UpdateReadyBatches() {
  while (slots_.HasNext()) {
    auto next_queue_id = slots_.Peek();
    if (batch_queues_.count(next_queue_id) == 0) {
      break;
    }
    auto& next_queue = batch_queues_.at(next_queue_id);
    if (!next_queue.HasNext()) {
      break;
    }
    auto slot_id = slots_.Next().first;
    auto batch_id = next_queue.Next().second;
    ready_batches_.emplace(slot_id, batch_id);
  }
}

} // namespace slog