#pragma once

#include <queue>
#include <random>
#include <unordered_map>

#include "common/configuration.h"
#include "common/types.h"
#include "data_structure/batch_log.h"
#include "module/base/networked_module.h"
#include "proto/transaction.pb.h"

namespace slog {

/**
 * A LocalLog buffers batch data from local partitions and outputs
 * the next local batch in the order given by the local Paxos process
 */
class LocalLog {
 public:
  void AddBatchId(uint32_t queue_id, uint32_t position, BatchId batch_id);
  void AddSlot(SlotId slot_id, uint32_t queue_id);

  bool HasNextBatch() const;
  std::pair<SlotId, BatchId> NextBatch();

  /* For debugging */
  size_t NumBufferedSlots() const { return slots_.NumBufferredItems(); }

  /* For debugging */
  std::unordered_map<uint32_t, size_t> NumBufferedBatchesPerQueue() const {
    std::unordered_map<uint32_t, size_t> queue_sizes;
    for (const auto& pair : batch_queues_) {
      queue_sizes.insert_or_assign(pair.first, pair.second.NumBufferredItems());
    }
    return queue_sizes;
  }

 private:
  void UpdateReadyBatches();

  // Used to decide the next queue to choose a batch from
  AsyncLog<uint32_t> slots_;
  // Batches from a partition form a queue
  std::unordered_map<uint32_t, AsyncLog<BatchId>> batch_queues_;
  // Chosen batches
  std::queue<std::pair<SlotId, BatchId>> ready_batches_;
};

class Interleaver : public NetworkedModule {
 public:
  Interleaver(const ConfigurationPtr& config, const std::shared_ptr<Broker>& broker,
              std::chrono::milliseconds poll_timeout = kModuleTimeout);

 protected:
  void HandleInternalRequest(EnvelopePtr&& env) final;

 private:
  void AdvanceLogs();

  void EmitBatch(BatchPtr&& batch);

  ConfigurationPtr config_;
  std::unordered_map<uint32_t, BatchLog> single_home_logs_;
  LocalLog local_log_;
};

}  // namespace slog