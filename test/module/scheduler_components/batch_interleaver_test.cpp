#include <gtest/gtest.h>

#include "module/scheduler_components/batch_interleaver.h"

using namespace std;
using namespace slog;

using internal::Batch;

TEST(BatchInterleaverTest, InOrder) {
  BatchInterleaver interleaver;
  interleaver.AddBatchId(111 /* queue_id */, 0 /* position */, 100 /* batch_id */);
  ASSERT_FALSE(interleaver.HasNextBatch());

  interleaver.AddSlot(0 /* slot_id */, 111 /* queue_id */);
  ASSERT_EQ(make_pair(0U, 100U), interleaver.NextBatch());

  interleaver.AddBatchId(222 /* queue_id */, 0 /* position */, 200 /* batch_id */);
  ASSERT_FALSE(interleaver.HasNextBatch());

  interleaver.AddSlot(1 /* slot_id */, 222 /* queue_id */);
  ASSERT_EQ(make_pair(1U, 200U), interleaver.NextBatch());

  ASSERT_FALSE(interleaver.HasNextBatch());
}

TEST(BatchInterleaverTest, BatchesComeFirst) {
  BatchInterleaver interleaver;

  interleaver.AddBatchId(222 /* queue_id */, 0 /* position */, 100 /* batch_id */);
  interleaver.AddBatchId(111 /* queue_id */, 0 /* position */, 200 /* batch_id */);
  interleaver.AddBatchId(333 /* queue_id */, 0 /* position */, 300 /* batch_id */);
  interleaver.AddBatchId(333 /* queue_id */, 1 /* position */, 400 /* batch_id */);

  interleaver.AddSlot(0 /* slot_id */, 111 /* queue_id */);
  ASSERT_EQ(make_pair(0U, 200U), interleaver.NextBatch());

  interleaver.AddSlot(1 /* slot_id */, 333 /* queue_id */);
  ASSERT_EQ(make_pair(1U, 300U), interleaver.NextBatch());

  interleaver.AddSlot(2 /* slot_id */, 222 /* queue_id */);
  ASSERT_EQ(make_pair(2U, 100U), interleaver.NextBatch());

  interleaver.AddSlot(3 /* slot_id */, 333 /* queue_id */);
  ASSERT_EQ(make_pair(3U, 400U), interleaver.NextBatch());

  ASSERT_FALSE(interleaver.HasNextBatch());
}

TEST(BatchInterleaverTest, SlotsComeFirst) {
  BatchInterleaver interleaver;

  interleaver.AddSlot(2 /* slot_id */, 222 /* queue_id */);
  interleaver.AddSlot(1 /* slot_id */, 333 /* queue_id */);
  interleaver.AddSlot(3 /* slot_id */, 333 /* queue_id */);
  interleaver.AddSlot(0 /* slot_id */, 111 /* queue_id */);

  interleaver.AddBatchId(111 /* queue_id */, 0 /* position */, 200 /* batch_id */);
  ASSERT_EQ(make_pair(0U, 200U), interleaver.NextBatch());

  interleaver.AddBatchId(333 /* queue_id */, 0 /* position */, 300 /* batch_id */);
  ASSERT_EQ(make_pair(1U, 300U), interleaver.NextBatch());

  interleaver.AddBatchId(222 /* queue_id */, 0 /* position */, 100 /* batch_id */);
  ASSERT_EQ(make_pair(2U, 100U), interleaver.NextBatch());

  interleaver.AddBatchId(333 /* queue_id */, 1 /* position */, 400 /* batch_id */);
  ASSERT_EQ(make_pair(3U, 400U), interleaver.NextBatch());

  ASSERT_FALSE(interleaver.HasNextBatch());
}

TEST(BatchInterleaverTest, MultipleNextBatches) {
  BatchInterleaver interleaver;

  interleaver.AddBatchId(111 /* queue_id */, 0 /* position */, 300 /* batch_id */);
  interleaver.AddBatchId(222 /* queue_id */, 0 /* position */, 100 /* batch_id */);
  interleaver.AddBatchId(333 /* queue_id */, 0 /* position */, 400 /* batch_id */);
  interleaver.AddBatchId(333 /* queue_id */, 1 /* position */, 200 /* batch_id */);

  interleaver.AddSlot(3 /* slot_id */, 333 /* queue_id */);
  interleaver.AddSlot(1 /* slot_id */, 333 /* queue_id */);
  interleaver.AddSlot(2 /* slot_id */, 111 /* queue_id */);
  interleaver.AddSlot(0 /* slot_id */, 222 /* queue_id */);

  ASSERT_EQ(make_pair(0U, 100U), interleaver.NextBatch());
  ASSERT_EQ(make_pair(1U, 400U), interleaver.NextBatch());
  ASSERT_EQ(make_pair(2U, 300U), interleaver.NextBatch());
  ASSERT_EQ(make_pair(3U, 200U), interleaver.NextBatch());

  ASSERT_FALSE(interleaver.HasNextBatch());
}

TEST(BatchInterleaverTest, SameOriginOutOfOrder) {
  BatchInterleaver interleaver;

  interleaver.AddBatchId(111 /* queue_id */, 1 /* position */, 200 /* batch_id */);
  interleaver.AddBatchId(111 /* queue_id */, 2 /* position */, 300 /* batch_id */);
  
  interleaver.AddSlot(0 /* slot_id */, 111 /* queue_id */);
  ASSERT_FALSE(interleaver.HasNextBatch());

  interleaver.AddSlot(1 /* slot_id */, 111 /* queue_id */);
  ASSERT_FALSE(interleaver.HasNextBatch());

  interleaver.AddBatchId(111 /* queue_id */, 0 /* position */, 100 /* batch_id */);

  interleaver.AddSlot(2 /* slot_id */, 111 /* queue_id */);
  ASSERT_TRUE(interleaver.HasNextBatch());


  ASSERT_EQ(make_pair(0U, 100U), interleaver.NextBatch());
  ASSERT_EQ(make_pair(1U, 200U), interleaver.NextBatch());
  ASSERT_EQ(make_pair(2U, 300U), interleaver.NextBatch());

  ASSERT_FALSE(interleaver.HasNextBatch());
}
