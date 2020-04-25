#include <vector>

#include <gtest/gtest.h>

#include "common/test_utils.h"
#include "common/proto_utils.h"

using namespace std;
using namespace slog;

internal::Request MakeBatch(
    BatchId batch_id,
    const vector<Transaction*>& txns,
    TransactionType batch_type) {
  internal::Request req;
  auto batch = req.mutable_forward_batch()->mutable_batch_data();
  batch->set_id(batch_id);
  batch->set_transaction_type(batch_type);
  for (auto txn : txns) {
    batch->mutable_transactions()->AddAllocated(txn);
  }
  return req;
}

internal::Request MakeLocalQueueOrder(uint32_t slot, uint32_t queue_id) {
  internal::Request req;
  req.mutable_local_queue_order()->set_slot(slot);
  req.mutable_local_queue_order()->set_queue_id(queue_id);
  return req;
}

class SchedulerTest : public ::testing::Test {
protected:
  static const size_t NUM_MACHINES = 6;

  void SetUp() {
    ConfigVec configs = MakeTestConfigurations(
        "scheduler",
        2 /* num_replicas */,
        3 /* num_partitions */);

    for (size_t i = 0; i < NUM_MACHINES; i++) {
      test_slogs_[i] = make_unique<TestSlog>(configs[i]);
      test_slogs_[i]->AddScheduler();
      input_[i] = test_slogs_[i]->AddChannel(SEQUENCER_CHANNEL);
      output_[i] = test_slogs_[i]->AddChannel(SERVER_CHANNEL);
    }
    // Relica 0
    test_slogs_[0]->Data("A", {"valueA", 0, 0});
    test_slogs_[0]->Data("D", {"valueD", 0, 0});
    test_slogs_[0]->Data("Y", {"valueY", 1, 0});

    test_slogs_[1]->Data("C", {"valueC", 0, 0});
    test_slogs_[1]->Data("F", {"valueF", 0, 0});
    test_slogs_[1]->Data("X", {"valueX", 1, 0});

    test_slogs_[2]->Data("B", {"valueB", 0, 0});
    test_slogs_[2]->Data("E", {"valueE", 0, 0});
    test_slogs_[2]->Data("Z", {"valueZ", 1, 0});

    // Replica 1
    test_slogs_[3]->Data("A", {"valueA", 0, 0});
    test_slogs_[3]->Data("D", {"valueD", 0, 0});
    test_slogs_[3]->Data("Y", {"valueY", 1, 0});

    test_slogs_[4]->Data("C", {"valueC", 0, 0});
    test_slogs_[4]->Data("F", {"valueF", 0, 0});
    test_slogs_[4]->Data("X", {"valueX", 1, 0});

    test_slogs_[5]->Data("B", {"valueB", 0, 0});
    test_slogs_[5]->Data("E", {"valueE", 0, 0});
    test_slogs_[5]->Data("Z", {"valueZ", 1, 0});

    for (const auto& test_slog : test_slogs_) {
      test_slog->StartInNewThreads();
    }
  }

  void SendSingleHomeBatch(
      BatchId batch_id,
      const vector<Transaction*>& txns,
      const vector<size_t>& partitions,
      // Use this when sending multiple batches
      size_t batch_order_offset = 0) {
    auto batch = MakeBatch(batch_id, txns, SINGLE_HOME);
    for (auto partition : partitions) {
      // Simulate a message containing a batch
      MMessage batch_msg;
      batch_msg.Set(MM_PROTO, batch);
      // Destination
      batch_msg.SetIdentity(MakeMachineIdAsString(0, partition));
      batch_msg.Set(MM_TO_CHANNEL, SCHEDULER_CHANNEL);
      // This message is sent from machine 0:0, so it will be queued up in queue 0
      input_[0]->Send(batch_msg);

      // Simulate a local paxos order message
      MMessage order_msg;
      order_msg.Set(MM_PROTO, MakeLocalQueueOrder(batch_order_offset, 0 /* queue_id */));
      order_msg.SetIdentity(MakeMachineIdAsString(0, partition));
      order_msg.Set(MM_TO_CHANNEL, SCHEDULER_CHANNEL);
      input_[0]->Send(order_msg);
    }
  }

  void SendMultiHomeBatch(
      BatchId batch_id,
      const vector<Transaction*>& txns,
      const vector<size_t>& partitions) {
    auto batch = MakeBatch(batch_id, txns, MULTI_HOME);
    for (auto partition : partitions) {
      // Simulate a message containing a batch. It doesn't matter where
      // this message comes from.
      MMessage batch_msg;
      batch_msg.Set(MM_PROTO, batch);
      batch_msg.Set(MM_TO_CHANNEL, SCHEDULER_CHANNEL);
      input_[partition]->Send(batch_msg);
      // Unlike single-home batch, multi-home batches are pre-ordered
      // and this order follows batch_id
    }
  }

  Transaction ReceiveMultipleAndMerge(uint32_t receiver, uint32_t num_partitions) {
    Transaction txn;
    bool first_time = true;
    for (uint32_t i = 0; i < num_partitions; i++) {
      MMessage msg;
      output_[receiver]->Receive(msg);
      internal::Request req;
      CHECK(msg.GetProto(req));
      CHECK_EQ(req.type_case(), internal::Request::kCompletedSubtxn);
      auto completed_subtxn = req.completed_subtxn();
      CHECK_EQ((uint32_t)completed_subtxn.involved_partitions_size(), num_partitions);
      auto sub_txn = completed_subtxn.txn();

      if (first_time) {
        txn = sub_txn;
      } else {
        MergeTransaction(txn, sub_txn);
      }

      first_time = false;
    }
    return txn;
  }

private:
  // Make sure to maintain the order of these variables otherwise the channels are
  // deallocated before the test slogs leading to ZMQ somehow hanging up.
  unique_ptr<Channel> input_[NUM_MACHINES];
  unique_ptr<Channel> output_[NUM_MACHINES];
  unique_ptr<TestSlog> test_slogs_[NUM_MACHINES];
};

TEST_F(SchedulerTest, SinglePartitionTransaction) {
  auto txn = MakeTransaction(
      {"A"}, /* read_set */
      {"D"},  /* write_set */
      "GET A     \n"
      "SET D newD\n", /* code */
      {},
      MakeMachineId("0:1") /* coordinating server */);
  txn->mutable_internal()->set_type(TransactionType::SINGLE_HOME);

  SendSingleHomeBatch(100, {txn}, {0});

  auto output_txn = ReceiveMultipleAndMerge(1, 1);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.read_set_size(), 1);
  ASSERT_EQ(output_txn.read_set().at("A"), "valueA");
  ASSERT_EQ(output_txn.write_set_size(), 1);
  ASSERT_EQ(output_txn.write_set().at("D"), "newD");
}

TEST_F(SchedulerTest, MultiPartitionTransaction1Active1Passive) {
  auto txn = MakeTransaction(
      {"A"}, /* read_set */
      {"C"},  /* write_set */
      "COPY A C" /* code */);
  txn->mutable_internal()->set_type(TransactionType::SINGLE_HOME);

  SendSingleHomeBatch(100, {txn}, {0, 1, 2});

  auto output_txn = ReceiveMultipleAndMerge(0, 2);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.read_set_size(), 1);
  ASSERT_EQ(output_txn.read_set().at("A"), "valueA");
  ASSERT_EQ(output_txn.write_set_size(), 1);
  ASSERT_EQ(output_txn.write_set().at("C"), "valueA");
}

TEST_F(SchedulerTest, MultiPartitionTransactionMutualWait2Partitions) {
  auto txn = MakeTransaction(
      {"B", "C"}, /* read_set */
      {"B", "C"},  /* write_set */
      "COPY C B\n"
      "COPY B C\n" /* code */);
  txn->mutable_internal()->set_type(TransactionType::SINGLE_HOME);

  SendSingleHomeBatch(100, {txn}, {0, 1, 2});

  auto output_txn = ReceiveMultipleAndMerge(0, 2);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.read_set_size(), 2);
  ASSERT_EQ(output_txn.read_set().at("B"), "valueB");
  ASSERT_EQ(output_txn.read_set().at("C"), "valueC");
  ASSERT_EQ(output_txn.write_set_size(), 2);
  ASSERT_EQ(output_txn.write_set().at("B"), "valueC");
  ASSERT_EQ(output_txn.write_set().at("C"), "valueB");
}

TEST_F(SchedulerTest, MultiPartitionTransactionWriteOnly) {
  auto txn = MakeTransaction(
      {}, /* read_set */
      {"A", "B", "C"},  /* write_set */
      "SET A newA\n"
      "SET B newB\n"
      "SET C newC\n" /* code */);
  txn->mutable_internal()->set_type(TransactionType::SINGLE_HOME);

  SendSingleHomeBatch(100, {txn}, {0, 1, 2});

  auto output_txn = ReceiveMultipleAndMerge(0, 3);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.read_set_size(), 0);
  ASSERT_EQ(output_txn.write_set_size(), 3);
  ASSERT_EQ(output_txn.write_set().at("A"), "newA");
  ASSERT_EQ(output_txn.write_set().at("B"), "newB");
  ASSERT_EQ(output_txn.write_set().at("C"), "newC");
}

TEST_F(SchedulerTest, MultiPartitionTransactionReadOnly) {
  auto txn = MakeTransaction(
      {"D", "E", "F"}, /* read_set */
      {},  /* write_set */
      "GET D\n"
      "GET E\n"
      "GET F\n" /* code */);
  txn->mutable_internal()->set_type(TransactionType::SINGLE_HOME);

  SendSingleHomeBatch(100, {txn}, {0, 1, 2});

  auto output_txn = ReceiveMultipleAndMerge(0, 3);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.read_set_size(), 3);
  ASSERT_EQ(output_txn.read_set().at("D"), "valueD");
  ASSERT_EQ(output_txn.read_set().at("E"), "valueE");
  ASSERT_EQ(output_txn.read_set().at("F"), "valueF");
  ASSERT_EQ(output_txn.write_set_size(), 0);
}

TEST_F(SchedulerTest, SimpleMultiHomeBatch) {
  auto txn = MakeTransaction(
      {"A", "X", "C"}, /* read_set */
      {"B", "Y", "Z"}, /* write_set */
      "GET A\n"
      "GET X\n"
      "GET C\n"
      "SET B newB\n"
      "SET Y newY\n"
      "SET Z newZ\n" /* code */);
  txn->mutable_internal()->set_type(TransactionType::MULTI_HOME);

  auto lo_txn_0 = MakeTransaction(
      {"A", "C"}, /* read_set */
      {"B"} /* write_set */,
      "" /* code */);
  lo_txn_0->mutable_internal()->set_type(TransactionType::LOCK_ONLY);

  auto lo_txn_1 = MakeTransaction(
      {"X"}, /* read_set */
      {"Y", "Z"}, /* write_set */
      ""/* code */);
  lo_txn_1->mutable_internal()->set_type(TransactionType::LOCK_ONLY);

  SendMultiHomeBatch(0, {txn}, {0, 1, 2});
  SendSingleHomeBatch(100, {lo_txn_0, lo_txn_1}, {0, 1, 2});

  auto output_txn = ReceiveMultipleAndMerge(0, 3);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.read_set_size(), 3);
  ASSERT_EQ(output_txn.read_set().at("A"), "valueA");
  ASSERT_EQ(output_txn.read_set().at("X"), "valueX");
  ASSERT_EQ(output_txn.read_set().at("C"), "valueC");
  ASSERT_EQ(output_txn.write_set_size(), 3);
  ASSERT_EQ(output_txn.write_set().at("B"), "newB");
  ASSERT_EQ(output_txn.write_set().at("Y"), "newY");
  ASSERT_EQ(output_txn.write_set().at("Z"), "newZ");
}

TEST_F(SchedulerTest, SinglePartitionTransactionValidateMasters) {
  auto txn = MakeTransaction(
      {"A"}, /* read_set */
      {"D"},  /* write_set */
      "GET A     \n"
      "SET D newD\n", /* code */
      {{"A", {0,0}}, {"D", {0,0}}},
      MakeMachineId("0:1") /* coordinating server */);
  txn->mutable_internal()->set_type(TransactionType::SINGLE_HOME);

  SendSingleHomeBatch(100, {txn}, {0});

  auto output_txn = ReceiveMultipleAndMerge(1, 1);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.read_set_size(), 1);
  ASSERT_EQ(output_txn.read_set().at("A"), "valueA");
  ASSERT_EQ(output_txn.write_set_size(), 1);
  ASSERT_EQ(output_txn.write_set().at("D"), "newD");
}

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  google::InstallFailureSignalHandler();
  return RUN_ALL_TESTS();
}