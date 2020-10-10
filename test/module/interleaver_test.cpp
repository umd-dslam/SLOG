#include <vector>

#include <gtest/gtest.h>

#include "common/test_utils.h"
#include "common/proto_utils.h"
#include "module/interleaver.h"

using namespace std;
using namespace slog;

using internal::Request;

TEST(LocalLogTest, InOrder) {
  LocalLog interleaver;
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

TEST(LocalLogTest, BatchesComeFirst) {
  LocalLog interleaver;

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

TEST(LocalLogTest, SlotsComeFirst) {
  LocalLog interleaver;

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

TEST(LocalLogTest, MultipleNextBatches) {
  LocalLog interleaver;

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

TEST(LocalLogTest, SameOriginOutOfOrder) {
  LocalLog interleaver;

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

const int NUM_REPLICAS = 2;
const int NUM_PARTITIONS = 2;
constexpr int NUM_MACHINES = NUM_REPLICAS * NUM_PARTITIONS;

class InterleaverTest : public ::testing::Test {
public:
  void SetUp() {
    auto configs = MakeTestConfigurations("interleaver", NUM_REPLICAS, NUM_PARTITIONS);
    for (int i = 0; i < 4; i++) {
      slogs_[i] = make_unique<TestSlog>(configs[i]);
      slogs_[i]->AddInterleaver();
      slogs_[i]->AddOutputChannel(kSchedulerChannel);
      senders_[i] = slogs_[i]->GetSender();
      slogs_[i]->StartInNewThreads();
    }
  }

  void SendToInterleaver(int from, int to, const internal::Request& req) {
    senders_[from]->Send(req, kInterleaverChannel, to);
  }

  Transaction* ReceiveTxn(int i) {
    Request req;
    if (!slogs_[i]->ReceiveFromOutputChannel(req, kSchedulerChannel)) {
      return nullptr;
    }
    if (req.type_case() != Request::kForwardTxn) {
      return nullptr;
    }
    return req.mutable_forward_txn()->release_txn();
  }

  unique_ptr<Sender> senders_[4];
  unique_ptr<TestSlog> slogs_[4];
};

internal::Batch* MakeBatch(
    BatchId batch_id,
    const vector<Transaction*>& txns,
    TransactionType batch_type) {
  internal::Batch* batch = new internal::Batch();
  batch->set_id(batch_id);
  batch->set_transaction_type(batch_type);
  for (auto txn : txns) {
    batch->mutable_transactions()->AddAllocated(txn);
  }
  return batch;
}

TEST_F(InterleaverTest, BatchDataBeforeBatchOrder) {
  auto expected_txn_1 = MakeTransaction({"A"}, {"B"}, "some code");
  auto expected_txn_2 = MakeTransaction({"X"}, {"Y"}, "");
  auto batch = MakeBatch(100, {expected_txn_1, expected_txn_2}, SINGLE_HOME);

  // Replicate batch data to all machines
  {

    Request req;
    auto forward_batch = req.mutable_forward_batch();
    forward_batch->mutable_batch_data()->CopyFrom(*batch);
    forward_batch->set_same_origin_position(0);

    for (int i = 0; i < NUM_MACHINES; i++) {
      SendToInterleaver(0, i, req);
    }
  }

  // Then send local ordering
  {
    Request req;
    auto local_queue_order = req.mutable_local_queue_order();
    local_queue_order->set_queue_id(0);
    local_queue_order->set_slot(0);
    SendToInterleaver(0, 0, req);
    SendToInterleaver(1, 1, req);

    // The batch order is replicated across all machines
    for (int i = 0; i < NUM_MACHINES; i++) {
      auto txn1 = ReceiveTxn(i);
      auto txn2 = ReceiveTxn(i);
      ASSERT_EQ(*txn1, *expected_txn_1);
      ASSERT_EQ(*txn2, *expected_txn_2);
      delete txn1;
      delete txn2;
    }
  }

  delete batch;
}

TEST_F(InterleaverTest, BatchOrderBeforeBatchData) {
  auto expected_txn_1 = MakeTransaction({"A"}, {"B"}, "some code");
  auto expected_txn_2 = MakeTransaction({"X"}, {"Y"}, "");
  auto batch = MakeBatch(100, {expected_txn_1, expected_txn_2}, SINGLE_HOME);

  // Then send local ordering
  {
    Request req;
    auto local_queue_order = req.mutable_local_queue_order();
    local_queue_order->set_queue_id(0);
    local_queue_order->set_slot(0);
    SendToInterleaver(0, 0, req);
    SendToInterleaver(1, 1, req);
  }

  // Replicate batch data to all machines
  {
    Request req;
    auto forward_batch = req.mutable_forward_batch();
    forward_batch->mutable_batch_data()->CopyFrom(*batch);
    forward_batch->set_same_origin_position(0);

    for (int i = 0; i < NUM_MACHINES; i++) {
      SendToInterleaver(0, i, req);
    }

    // Both batch data and batch order are sent at the same time
    for (int i = 0; i < NUM_MACHINES; i++) {
      auto txn1 = ReceiveTxn(i);
      auto txn2 = ReceiveTxn(i);
      ASSERT_EQ(*txn1, *expected_txn_1);
      ASSERT_EQ(*txn2, *expected_txn_2);
      delete txn1;
      delete txn2;
    }
  }

  delete batch;
}

TEST_F(InterleaverTest, ThreeBatches) {
  auto sh_txn_1 = MakeTransaction({"A"}, {"B"}, "some code");
  auto sh_batch_1 = MakeBatch(100, {sh_txn_1}, SINGLE_HOME);

  auto sh_txn_2 = MakeTransaction({"M"}, {"N"}, "");
  auto sh_batch_2 = MakeBatch(200, {sh_txn_2}, SINGLE_HOME);

  auto mh_txn = MakeTransaction({"X"}, {"Y"}, "");
  auto mh_batch = MakeBatch(0, {mh_txn}, MULTI_HOME);

  // Replicate batch data to all machines
  {
    Request req1;
    auto forward_batch1 = req1.mutable_forward_batch();
    forward_batch1->mutable_batch_data()->CopyFrom(*sh_batch_1);
    forward_batch1->set_same_origin_position(0);

    Request req2;
    auto forward_batch2 = req2.mutable_forward_batch();
    forward_batch2->mutable_batch_data()->CopyFrom(*sh_batch_2);
    forward_batch2->set_same_origin_position(0);

    Request req3;
    auto forward_batch3 = req3.mutable_forward_batch();
    forward_batch3->mutable_batch_data()->CopyFrom(*mh_batch);

    for (int i = 0; i < NUM_MACHINES; i++) {
      SendToInterleaver(0, i, req1);
      SendToInterleaver(1, i, req2);
      SendToInterleaver(0, i, req3);
    }

    // The multi-home batch can be emitted immediately
    for (int i = 0; i < NUM_MACHINES; i++) {
      auto txn = ReceiveTxn(i);
      ASSERT_EQ(*txn, *mh_txn);
      delete txn;
    }
  }

  // Then send local ordering. Txn 1 is ordered after txn 2
  {
    Request req1;
    auto local_queue_order1 = req1.mutable_local_queue_order();
    local_queue_order1->set_slot(0);
    local_queue_order1->set_queue_id(1);
    SendToInterleaver(0, 0, req1);
    SendToInterleaver(1, 1, req1);

    for (int i = 0; i < NUM_MACHINES; i++) {
      auto txn = ReceiveTxn(i);
      ASSERT_EQ(*txn, *sh_txn_2);
      delete txn;
    }

    Request req2;
    auto local_queue_order2 = req2.mutable_local_queue_order();
    local_queue_order2->set_slot(1);
    local_queue_order2->set_queue_id(0);
    SendToInterleaver(0, 0, req2);
    SendToInterleaver(1, 1, req2);

    for (int i = 0; i < NUM_MACHINES; i++) {
      auto txn = ReceiveTxn(i);
      ASSERT_EQ(*txn, *sh_txn_1);
      delete txn;
    }
  } 
}
