#include <gtest/gtest.h>

#include "module/scheduler_components/local_log.h"

using namespace std;
using namespace slog;

using internal::Batch;

class LocalLogTest : public ::testing::Test {
protected:
  static const size_t NUM_BATCHES = 3;

  void SetUp() {
    for (size_t i = 0; i < NUM_BATCHES; i++) {
      batches_[i] = make_shared<Batch>();
      batches_[i]->set_id(100 * (i + 1));
    }
  }

  void AssertBatchId(uint32_t expected_id, BatchPtr batch) {
    ASSERT_NE(nullptr, batch);
    ASSERT_EQ(expected_id, batch->id());
  }

  BatchPtr batches_[NUM_BATCHES]; // batch ids: 100 200 300
};

TEST_F(LocalLogTest, InOrder) {
  LocalLog log;
  log.AddBatch(0 /* slot_id */, batches_[0]);
  AssertBatchId(100, log.NextBatch());

  log.AddBatch(1 /* slot_id */, batches_[1]);
  AssertBatchId(200, log.NextBatch());

  log.AddBatch(2 /* slot_id */, batches_[2]);
  AssertBatchId(300, log.NextBatch());

  ASSERT_FALSE(log.HasNextBatch());
}

TEST_F(LocalLogTest, OutOfOrder) {
  LocalLog log;
  log.AddBatch(1 /* slot_id */, batches_[1]);
  ASSERT_FALSE(log.HasNextBatch());

  log.AddBatch(0 /* slot_id */, batches_[0]);
  AssertBatchId(100, log.NextBatch());
  AssertBatchId(200, log.NextBatch());

  log.AddBatch(2 /* slot_id */, batches_[2]);
  AssertBatchId(300, log.NextBatch());

  ASSERT_FALSE(log.HasNextBatch());
}

TEST_F(LocalLogTest, MultipleNextBatches) {
  LocalLog log;

  log.AddBatch(2 /* slot_id */, batches_[2]);
  log.AddBatch(1 /* slot_id */, batches_[1]);
  log.AddBatch(0 /* slot_id */, batches_[0]);

  AssertBatchId(100, log.NextBatch());
  AssertBatchId(200, log.NextBatch());
  AssertBatchId(300, log.NextBatch());
  ASSERT_FALSE(log.HasNextBatch());
}