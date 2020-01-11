#include "module/scheduler_components/local_log.h"

#include <glog/logging.h>

namespace slog {

LocalLog::LocalLog() : next_slot_(0) {}

void LocalLog::AddBatch(SlotId slot_id, BatchPtr batch) {
  if (batches_.count(slot_id) > 0) {
    LOG(ERROR) << "LocalLog: slot " << slot_id << " has already been taken.";
    return;
  }
  batches_[slot_id] = batch;
}

bool LocalLog::HasNextBatch() const {
  return batches_.count(next_slot_) > 0;
}

BatchPtr LocalLog::NextBatch() {
  if (!HasNextBatch()) {
    throw std::runtime_error("NextBatch() was called when there is no batch");
  }
  auto next_batch = batches_[next_slot_];
  batches_.erase(next_slot_);
  next_slot_++;

  return next_batch;
}

} // namespace slog