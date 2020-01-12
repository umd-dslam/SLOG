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
      pair<SlotId, uint32_t> expected, pair<SlotId, BatchPtr> batch) {
    ASSERT_NE(nullptr, batch.second);
    ASSERT_EQ(expected.first, batch.first);
    ASSERT_EQ(expected.second, batch.second->id());
  }

  BatchPtr batches_[NUM_BATCHES]; // batch ids: 100 200 300 400
};

TEST_F(BatchInterleaverTest, InOrder) {
  BatchInterleaver interleaver;
  interleaver.AddBatch(111 /* queue_id */, batches_[0]);
  ASSERT_FALSE(interleaver.HasNextBatch());

  interleaver.AddAgreedSlot(0 /* slot_id */, 111 /* queue_id */);
  AssertBatchId({0, 100}, interleaver.NextBatch());

  interleaver.AddBatch(222 /* queue_id */, batches_[1]);
  ASSERT_FALSE(interleaver.HasNextBatch());

  interleaver.AddAgreedSlot(1 /* slot_id */, 222 /* queue_id */);
  AssertBatchId({1, 200}, interleaver.NextBatch());

  ASSERT_FALSE(interleaver.HasNextBatch());
}

TEST_F(BatchInterleaverTest, BatchesComeFirst) {
  BatchInterleaver interleaver;

  interleaver.AddBatch(222 /* queue_id */, batches_[0]);
  interleaver.AddBatch(111 /* queue_id */, batches_[1]);
  interleaver.AddBatch(333 /* queue_id */, batches_[2]);
  interleaver.AddBatch(333 /* queue_id */, batches_[3]);

  interleaver.AddAgreedSlot(0 /* slot_id */, 111 /* queue_id */);
  AssertBatchId({0, 200}, interleaver.NextBatch());

  interleaver.AddAgreedSlot(1 /* slot_id */, 333 /* queue_id */);
  AssertBatchId({1, 300}, interleaver.NextBatch());

  interleaver.AddAgreedSlot(2 /* slot_id */, 222 /* queue_id */);
  AssertBatchId({2, 100}, interleaver.NextBatch());

  interleaver.AddAgreedSlot(3 /* slot_id */, 333 /* queue_id */);
  AssertBatchId({3, 400}, interleaver.NextBatch());

  ASSERT_FALSE(interleaver.HasNextBatch());
}

TEST_F(BatchInterleaverTest, SlotsComeFirst) {
  BatchInterleaver interleaver;

  interleaver.AddAgreedSlot(2 /* slot_id */, 222 /* queue_id */);
  interleaver.AddAgreedSlot(1 /* slot_id */, 333 /* queue_id */);
  interleaver.AddAgreedSlot(3 /* slot_id */, 333 /* queue_id */);
  interleaver.AddAgreedSlot(0 /* slot_id */, 111 /* queue_id */);

  interleaver.AddBatch(111 /* queue_id */, batches_[1]);
  AssertBatchId({0, 200}, interleaver.NextBatch());

  interleaver.AddBatch(333 /* queue_id */, batches_[2]);
  AssertBatchId({1, 300}, interleaver.NextBatch());

  interleaver.AddBatch(222 /* queue_id */, batches_[0]);
  AssertBatchId({2, 100}, interleaver.NextBatch());

  interleaver.AddBatch(333 /* queue_id */, batches_[3]);
  AssertBatchId({3, 400}, interleaver.NextBatch());

  ASSERT_FALSE(interleaver.HasNextBatch());
}

TEST_F(BatchInterleaverTest, MultipleNextBatches) {
  BatchInterleaver interleaver;

  interleaver.AddBatch(111 /* queue_id */, batches_[2]);
  interleaver.AddBatch(222 /* queue_id */, batches_[0]);
  interleaver.AddBatch(333 /* queue_id */, batches_[3]);
  interleaver.AddBatch(333 /* queue_id */, batches_[1]);

  interleaver.AddAgreedSlot(3 /* slot_id */, 333 /* queue_id */);
  interleaver.AddAgreedSlot(1 /* slot_id */, 333 /* queue_id */);
  interleaver.AddAgreedSlot(2 /* slot_id */, 111 /* queue_id */);
  interleaver.AddAgreedSlot(0 /* slot_id */, 222 /* queue_id */);

  AssertBatchId({0, 100}, interleaver.NextBatch());
  AssertBatchId({1, 400}, interleaver.NextBatch());
  AssertBatchId({2, 300}, interleaver.NextBatch());
  AssertBatchId({3, 200}, interleaver.NextBatch());

  ASSERT_FALSE(interleaver.HasNextBatch());
}