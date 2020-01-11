#include "batch_interleaver.h"

#include <glog/logging.h>

namespace slog {

BatchInterleaver::BatchInterleaver() : next_slot_(0) {}

void BatchInterleaver::AddBatch(uint32_t queue_id, BatchPtr batch) {
  batch_queues_[queue_id].push_back(batch);
  UpdateReadyBatches();
}

void BatchInterleaver::AddAgreedSlot(SlotId slot_id, uint32_t queue_id) {
  if (pending_slots_.count(slot_id) > 0) {
    LOG(ERROR) << "BatchInterleaver: slot " << slot_id << " has already been taken.";
    return;
  }
  pending_slots_[slot_id] = queue_id;
  UpdateReadyBatches();
}

bool BatchInterleaver::HasNextBatch() const {
  return !ready_batches_.empty();
}

pair<BatchPtr, SlotId> BatchInterleaver::NextBatch() {
  if (!HasNextBatch()) {
    throw std::runtime_error("NextBatch() was called when there is no batch");
  }
  auto next_batch = ready_batches_.front();
  ready_batches_.pop_front();
  return next_batch;
}

void BatchInterleaver::UpdateReadyBatches() {
  while (pending_slots_.count(next_slot_) > 0) {
    auto next_queue_id = pending_slots_.at(next_slot_);
    if (batch_queues_[next_queue_id].empty()) {
      break;
    }

    auto next_batch = batch_queues_.at(next_queue_id).front();
    ready_batches_.emplace_back(next_batch, next_slot_);

    batch_queues_[next_queue_id].pop_front();
    pending_slots_.erase(next_slot_);
    next_slot_++;
  }
}

} // namespace slog