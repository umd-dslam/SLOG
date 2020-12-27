#include "data_structure/batch_log.h"

#include <gtest/gtest.h>

using namespace std;
using namespace slog;

using internal::Batch;

class BatchLogTest : public ::testing::Test {
 protected:
  static const size_t NUM_BATCHES = 3;

  void SetUp() {
    for (size_t i = 0; i < NUM_BATCHES; i++) {
      batches[i] = make_unique<Batch>();
      batches[i]->set_id(100 * (i + 1));
    }
  }

  bool BatchEQ(pair<uint32_t, uint32_t> expected_id, pair<SlotId, BatchPtr> batch) {
    return batch.second != nullptr && expected_id.second == batch.second->id() && expected_id.first == batch.first;
  }

  BatchPtr batches[NUM_BATCHES];  // batch ids: 100 200 300
};

TEST_F(BatchLogTest, InOrder) {
  BatchLog log;
  log.AddSlot(0 /* slot_id */, 100 /* batch_id */);
  log.AddBatch(move(batches[0]));
  ASSERT_TRUE(BatchEQ({0, 100}, log.NextBatch()));

  log.AddSlot(1 /* slot_id */, 200 /* batch_id */);
  log.AddBatch(move(batches[1]));
  ASSERT_TRUE(BatchEQ({1, 200}, log.NextBatch()));

  log.AddSlot(2 /* slot_id */, 300 /* batch_id */);
  log.AddBatch(move(batches[2]));
  ASSERT_TRUE(BatchEQ({2, 300}, log.NextBatch()));

  ASSERT_FALSE(log.HasNextBatch());
}

TEST_F(BatchLogTest, OutOfOrder) {
  BatchLog log;
  log.AddBatch(move(batches[1]));
  ASSERT_FALSE(log.HasNextBatch());

  log.AddBatch(move(batches[0]));
  ASSERT_FALSE(log.HasNextBatch());

  log.AddSlot(1 /* slot_id */, 100 /* batch_id */);
  ASSERT_FALSE(log.HasNextBatch());

  log.AddSlot(0 /* slot_id */, 200 /* batch_id */);
  ASSERT_TRUE(BatchEQ({0, 200}, log.NextBatch()));
  ASSERT_TRUE(BatchEQ({1, 100}, log.NextBatch()));

  ASSERT_FALSE(log.HasNextBatch());
}

TEST_F(BatchLogTest, MultipleNextBatches) {
  BatchLog log;

  log.AddBatch(move(batches[2]));
  log.AddBatch(move(batches[1]));
  log.AddBatch(move(batches[0]));
  log.AddSlot(2 /* slot_id */, 300 /* batch_id */);
  log.AddSlot(1 /* slot_id */, 200 /* batch_id */);
  log.AddSlot(0 /* slot_id */, 100 /* batch_id */);

  ASSERT_TRUE(BatchEQ({0, 100}, log.NextBatch()));
  ASSERT_TRUE(BatchEQ({1, 200}, log.NextBatch()));
  ASSERT_TRUE(BatchEQ({2, 300}, log.NextBatch()));
  ASSERT_FALSE(log.HasNextBatch());
}