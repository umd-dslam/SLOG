#include <gtest/gtest.h>

#include "common/async_log.h"

using namespace std;
using namespace slog;

using internal::Batch;

class AsyncLogTest : public ::testing::Test {
protected:
  static const size_t NUM_BATCHES = 3;

  void SetUp() {
    for (size_t i = 0; i < NUM_BATCHES; i++) {
      batches[i] = make_unique<Batch>();
      batches[i]->set_id(100 * (i + 1));
    }
  }

  void AssertBatchId(uint32_t expected_id, BatchPtr batch) {
    ASSERT_NE(nullptr, batch);
    ASSERT_EQ(expected_id, batch->id());
  }

  BatchPtr batches[NUM_BATCHES]; // batch ids: 100 200 300
};

TEST_F(AsyncLogTest, InOrder) {
  AsyncLog log;
  log.AddSlot(0 /* slot_id */, 100 /* batch_id */);
  log.AddBatch(move(batches[0]));
  AssertBatchId(100, log.NextBatch());

  log.AddSlot(1 /* slot_id */, 200 /* batch_id */);
  log.AddBatch(move(batches[1]));
  AssertBatchId(200, log.NextBatch());

  log.AddSlot(2 /* slot_id */, 300 /* batch_id */);
  log.AddBatch(move(batches[2]));
  AssertBatchId(300, log.NextBatch());

  ASSERT_FALSE(log.HasNextBatch());
}

TEST_F(AsyncLogTest, OutOfOrder) {
  AsyncLog log;
  log.AddBatch(move(batches[1]));
  ASSERT_FALSE(log.HasNextBatch());

  log.AddBatch(move(batches[0]));
  ASSERT_FALSE(log.HasNextBatch());

  log.AddSlot(1 /* slot_id */, 100 /* batch_id */);
  ASSERT_FALSE(log.HasNextBatch());

  log.AddSlot(0 /* slot_id */, 200 /* batch_id */);
  AssertBatchId(200, log.NextBatch());
  AssertBatchId(100, log.NextBatch());

  ASSERT_FALSE(log.HasNextBatch());
}

TEST_F(AsyncLogTest, MultipleNextBatches) {
  AsyncLog log;

  log.AddBatch(move(batches[2]));
  log.AddBatch(move(batches[1]));
  log.AddBatch(move(batches[0]));
  log.AddSlot(2 /* slot_id */, 300);
  log.AddSlot(1 /* slot_id */, 200);
  log.AddSlot(0 /* slot_id */, 100);

  AssertBatchId(100, log.NextBatch());
  AssertBatchId(200, log.NextBatch());
  AssertBatchId(300, log.NextBatch());
  ASSERT_FALSE(log.HasNextBatch());
}