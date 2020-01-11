#include <gtest/gtest.h>

#include "module/scheduler_components/batch_interleaver.h"

using namespace std;
using namespace slog;

using internal::Batch;

class BatchInterleaverTest : public ::testing::Test {
protected:
  static const size_t NUM_BATCHES = 4;

  void SetUp() {
    for (size_t i = 0; i < NUM_BATCHES; i++) {
      batches_[i] = make_shared<Batch>();
      batches_[i]->set_id(100 * (i + 1));
    }
  }

  void AssertBatchId(
      pair<uint32_t, SlotId> expected, pair<BatchPtr, SlotId> batch) {
    ASSERT_NE(nullptr, batch.first);
    ASSERT_EQ(expected.first, batch.first->id());
    ASSERT_EQ(expected.second, batch.second);
  }

  BatchPtr batches_[NUM_BATCHES]; // batch ids: 100 200 300 400
};

TEST_F(BatchInterleaverTest, InOrder) {
  BatchInterleaver interleaver;
  interleaver.AddBatch(111 /* queue_id */, batches_[0]);
  ASSERT_FALSE(interleaver.HasReadyBatch());

  interleaver.AddAgreedSlot(0 /* slot_id */, 111 /* queue_id */);
  AssertBatchId({100, 0}, interleaver.NextBatch());

  interleaver.AddBatch(222 /* queue_id */, batches_[1]);
  ASSERT_FALSE(interleaver.HasReadyBatch());

  interleaver.AddAgreedSlot(1 /* slot_id */, 222 /* queue_id */);
  AssertBatchId({200, 1}, interleaver.NextBatch());

  ASSERT_FALSE(interleaver.HasReadyBatch());
}

TEST_F(BatchInterleaverTest, BatchesComeFirst) {
  BatchInterleaver interleaver;

  interleaver.AddBatch(222 /* queue_id */, batches_[0]);
  interleaver.AddBatch(111 /* queue_id */, batches_[1]);
  interleaver.AddBatch(333 /* queue_id */, batches_[2]);
  interleaver.AddBatch(333 /* queue_id */, batches_[3]);

  interleaver.AddAgreedSlot(0 /* slot_id */, 111 /* queue_id */);
  AssertBatchId({200, 0}, interleaver.NextBatch());

  interleaver.AddAgreedSlot(1 /* slot_id */, 333 /* queue_id */);
  AssertBatchId({300, 1}, interleaver.NextBatch());

  interleaver.AddAgreedSlot(2 /* slot_id */, 222 /* queue_id */);
  AssertBatchId({100, 2}, interleaver.NextBatch());

  interleaver.AddAgreedSlot(3 /* slot_id */, 333 /* queue_id */);
  AssertBatchId({400, 3}, interleaver.NextBatch());

  ASSERT_FALSE(interleaver.HasReadyBatch());
}

TEST_F(BatchInterleaverTest, SlotsComeFirst) {
  BatchInterleaver interleaver;

  interleaver.AddAgreedSlot(2 /* slot_id */, 222 /* queue_id */);
  interleaver.AddAgreedSlot(1 /* slot_id */, 333 /* queue_id */);
  interleaver.AddAgreedSlot(3 /* slot_id */, 333 /* queue_id */);
  interleaver.AddAgreedSlot(0 /* slot_id */, 111 /* queue_id */);

  interleaver.AddBatch(111 /* queue_id */, batches_[1]);
  AssertBatchId({200, 0}, interleaver.NextBatch());

  interleaver.AddBatch(333 /* queue_id */, batches_[2]);
  AssertBatchId({300, 1}, interleaver.NextBatch());

  interleaver.AddBatch(222 /* queue_id */, batches_[0]);
  AssertBatchId({100, 2}, interleaver.NextBatch());

  interleaver.AddBatch(333 /* queue_id */, batches_[3]);
  AssertBatchId({400, 3}, interleaver.NextBatch());

  ASSERT_FALSE(interleaver.HasReadyBatch());
}

TEST_F(BatchInterleaverTest, MultipleReadyBatches) {
  BatchInterleaver interleaver;

  interleaver.AddBatch(111 /* queue_id */, batches_[2]);
  interleaver.AddBatch(222 /* queue_id */, batches_[0]);
  interleaver.AddBatch(333 /* queue_id */, batches_[3]);
  interleaver.AddBatch(333 /* queue_id */, batches_[1]);

  interleaver.AddAgreedSlot(3 /* slot_id */, 333 /* queue_id */);
  interleaver.AddAgreedSlot(1 /* slot_id */, 333 /* queue_id */);
  interleaver.AddAgreedSlot(2 /* slot_id */, 111 /* queue_id */);
  interleaver.AddAgreedSlot(0 /* slot_id */, 222 /* queue_id */);

  AssertBatchId({100, 0}, interleaver.NextBatch());
  AssertBatchId({400, 1}, interleaver.NextBatch());
  AssertBatchId({300, 2}, interleaver.NextBatch());
  AssertBatchId({200, 3}, interleaver.NextBatch());

  ASSERT_FALSE(interleaver.HasReadyBatch());
}