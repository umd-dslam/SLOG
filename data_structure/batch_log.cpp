#include "data_structure/batch_log.h"

#include <glog/logging.h>

namespace slog {

BatchLog::BatchLog() {}

void BatchLog::AddBatch(BatchPtr&& batch) {
  batches_[batch->id()] = std::move(batch);
  UpdateReadyBatches();
}

void BatchLog::AddSlot(SlotId slot_id, BatchId batch_id) {
  slots_.Insert(slot_id, batch_id);
  UpdateReadyBatches();
}

bool BatchLog::HasNextBatch() const {
  return !ready_batches_.empty();
}

std::pair<SlotId, BatchPtr> BatchLog::NextBatch() {
  if (!HasNextBatch()) {
    throw std::runtime_error("NextBatch() was called when there is no batch");
  }
  auto next_slot = ready_batches_.front().first;
  auto next_batch_id = ready_batches_.front().second;
  ready_batches_.pop();

  auto next_batch = std::move(batches_[next_batch_id]);
  batches_.erase(next_batch_id);

  return {next_slot, std::move(next_batch)};
}

void BatchLog::UpdateReadyBatches() {
  while (slots_.HasNext()) {
    auto next_batch_id = slots_.Peek();
    if (batches_.count(next_batch_id) == 0) {
      break;
    }
    ready_batches_.push(slots_.Next());
  }
}

} // namespace slog