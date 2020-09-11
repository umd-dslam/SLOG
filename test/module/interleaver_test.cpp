#include <vector>

#include <gtest/gtest.h>

#include "common/test_utils.h"
#include "common/proto_utils.h"

using namespace std;
using namespace slog;

using internal::Request;

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
      slogs_[i]->AddOutputChannel(SCHEDULER_CHANNEL);
      senders_[i] = slogs_[i]->GetSender();
      slogs_[i]->StartInNewThreads();
    }
  }

  void SendToInterleaver(int from, int to, const internal::Request& req) {
    auto to_machine = MakeMachineIdAsString(to / NUM_PARTITIONS, to % NUM_PARTITIONS);
    senders_[from]->Send(req, INTERLEAVER_CHANNEL, to_machine);
  }

  internal::Batch* ReceiveBatch(int i) {
    auto forward_batch = ReceiveBatchMessage(i);
    if (forward_batch->part_case() != internal::ForwardBatch::kBatchData) {
      return nullptr;
    }
    return forward_batch->release_batch_data();
  }

  internal::BatchOrder* ReceiveBatchOrder(int i) {
    auto forward_batch = ReceiveBatchMessage(i);
    if (forward_batch->part_case() != internal::ForwardBatch::kBatchOrder) {
      return nullptr;
    }
    return forward_batch->release_batch_order();
  }

  internal::ForwardBatch* ReceiveBatchMessage(int i) {
    MMessage msg;
    slogs_[i]->ReceiveFromOutputChannel(msg, SCHEDULER_CHANNEL);
    Request req;
    if (!msg.GetProto(req)) {
      return nullptr;
    }
    if (req.type_case() != Request::kForwardBatch) {
      return nullptr;
    }
    return req.release_forward_batch();
  }

  unique_ptr<Sender> senders_[4];
  unique_ptr<TestSlog> slogs_[4];
};

internal::Batch MakeBatch(int id) {
  internal::Batch batch;
  auto txn = MakeTransaction({"A"},{"B"}, "some code");
  txn->mutable_internal()->set_type(TransactionType::SINGLE_HOME);
  batch.mutable_transactions()->AddAllocated(txn);
  batch.set_id(id);
  batch.set_transaction_type(TransactionType::SINGLE_HOME);
  return batch;
}

TEST_F(InterleaverTest, BatchDataBeforeBatchOrder) {
  // Replicate batch data to all machines
  {
    Request req;
    auto forward_batch = req.mutable_forward_batch();
    forward_batch->mutable_batch_data()->CopyFrom(MakeBatch(100));
    forward_batch->set_same_origin_position(0);

    for (int i = 0; i < NUM_MACHINES; i++) {
      SendToInterleaver(0, i, req);
    }

    // The batch data must be immediately forwarded to the scheduler in each machine
    for (int i = 0; i < NUM_MACHINES; i++) {
      auto batch = ReceiveBatch(i);
      ASSERT_NE(batch, nullptr);
      ASSERT_EQ(batch->transactions_size(), 1);
      delete batch;
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
      auto order = ReceiveBatchOrder(i);
      ASSERT_NE(order, nullptr);
      ASSERT_EQ(order->batch_id(), 100);
      ASSERT_EQ(order->slot(), 0);
      delete order;
    }
  } 
}

TEST_F(InterleaverTest, BatchOrderBeforeBatchData) {
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
    forward_batch->mutable_batch_data()->CopyFrom(MakeBatch(100));
    forward_batch->set_same_origin_position(0);

    for (int i = 0; i < NUM_MACHINES; i++) {
      SendToInterleaver(0, i, req);
    }

    // Both batch data and batch order are sent at the same time
    for (int i = 0; i < NUM_MACHINES; i++) {
      for (int j = 0; j < 2; j++) {
        auto msg = ReceiveBatchMessage(i);
        if (msg->part_case() == internal::ForwardBatch::kBatchData) {
          auto& batch = msg->batch_data();
          ASSERT_EQ(batch.transactions_size(), 1);
        } else {
          auto& order = msg->batch_order();
          ASSERT_EQ(order.batch_id(), 100);
          ASSERT_EQ(order.slot(), 0);
        }
      }
    }
  }
}

TEST_F(InterleaverTest, TwoBatches) {
  // Replicate first batch data to all machines
  {
    Request req1;
    auto forward_batch1 = req1.mutable_forward_batch();
    forward_batch1->mutable_batch_data()->CopyFrom(MakeBatch(100));
    forward_batch1->set_same_origin_position(0);

    Request req2;
    auto forward_batch2 = req2.mutable_forward_batch();
    forward_batch2->mutable_batch_data()->CopyFrom(MakeBatch(200));
    forward_batch2->set_same_origin_position(0);


    for (int i = 0; i < NUM_MACHINES; i++) {
      SendToInterleaver(0, i, req1);
      SendToInterleaver(1, i, req2);
    }

    // The batch data must be immediately forwarded to the scheduler in each machine
    for (int i = 0; i < NUM_MACHINES; i++) {
      auto batch1 = ReceiveBatch(i);
      ASSERT_NE(batch1, nullptr);
      ASSERT_EQ(batch1->transactions_size(), 1);
      delete batch1;

      auto batch2 = ReceiveBatch(i);
      ASSERT_NE(batch2, nullptr);
      ASSERT_EQ(batch2->transactions_size(), 1);
      delete batch2;
    }
  }

  // Then send local ordering
  {
    Request req1;
    auto local_queue_order1 = req1.mutable_local_queue_order();
    local_queue_order1->set_slot(0);
    local_queue_order1->set_queue_id(1);
    SendToInterleaver(0, 0, req1);
    SendToInterleaver(1, 1, req1);

    for (int i = 0; i < NUM_MACHINES; i++) {
      auto order = ReceiveBatchOrder(i);
      ASSERT_NE(order, nullptr);
      ASSERT_EQ(order->batch_id(), 200);
      ASSERT_EQ(order->slot(), 0);
      delete order;
    }

    Request req2;
    auto local_queue_order2 = req2.mutable_local_queue_order();
    local_queue_order2->set_slot(1);
    local_queue_order2->set_queue_id(0);
    SendToInterleaver(0, 0, req2);
    SendToInterleaver(1, 1, req2);

    for (int i = 0; i < NUM_MACHINES; i++) {
      auto order = ReceiveBatchOrder(i);
      ASSERT_NE(order, nullptr);
      ASSERT_EQ(order->batch_id(), 100);
      ASSERT_EQ(order->slot(), 1);
      delete order;
    }
  } 
}
