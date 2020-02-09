#include <vector>

#include <gtest/gtest.h>

#include "common/test_utils.h"
#include "common/proto_utils.h"

using namespace std;
using namespace slog;

internal::Request MakeSingleHomeBatch(
    BatchId batch_id, const vector<Transaction>& txns) {
  internal::Request req;
  auto batch = req.mutable_forward_batch()->mutable_batch_data();
  batch->set_id(batch_id);
  batch->set_transaction_type(TransactionType::SINGLE_HOME);
  for (auto txn : txns) {
    batch->mutable_transactions()->Add(move(txn));
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
  static const size_t NUM_MACHINES = 3;

  void SetUp() {
    ConfigVec configs = MakeTestConfigurations(
        "scheduler",
        1 /* num_replicas */,
        3 /* num_partitions */);

    for (size_t i = 0; i < NUM_MACHINES; i++) {
      test_slogs_[i] = make_unique<TestSlog>(configs[i]);
      test_slogs_[i]->AddScheduler();
      input_[i] = test_slogs_[i]->AddChannel(SEQUENCER_CHANNEL);
      output_[i] = test_slogs_[i]->AddChannel(SERVER_CHANNEL);
    }
    test_slogs_[0]->Data("B", {"valueB", 0, 0});
    test_slogs_[0]->Data("E", {"valueE", 0, 0});
    test_slogs_[1]->Data("A", {"valueA", 0, 0});
    test_slogs_[1]->Data("D", {"valueD", 0, 0});
    test_slogs_[2]->Data("C", {"valueC", 0, 0});
    test_slogs_[2]->Data("F", {"valueF", 0, 0});

    for (const auto& test_slog : test_slogs_) {
      test_slog->StartInNewThreads();
    }
  }

  void SendBatch(
        BatchId batch_id,
        const vector<Transaction>& txns,
        const vector<size_t>& partitions) {
    for (auto partition : partitions) {
      MMessage msg;
      msg.Set(MM_PROTO, MakeSingleHomeBatch(batch_id, txns));
      msg.Set(MM_TO_CHANNEL, SCHEDULER_CHANNEL);
      input_[partition]->Send(msg);

      msg.Clear();
      msg.Set(MM_PROTO, MakeLocalQueueOrder(0, partition));
      msg.Set(MM_TO_CHANNEL, SCHEDULER_CHANNEL);
      input_[partition]->Send(msg);
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
      CHECK_EQ(req.type_case(), internal::Request::kForwardSubTxn);
      auto forward_sub_txn = req.forward_sub_txn();
      CHECK_EQ(forward_sub_txn.involved_partitions_size(), num_partitions);
      auto sub_txn = forward_sub_txn.txn();

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
  txn.mutable_internal()->set_type(TransactionType::SINGLE_HOME);

  SendBatch(100, {txn}, {0});

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
  txn.mutable_internal()->set_type(TransactionType::SINGLE_HOME);

  SendBatch(100, {txn}, {0, 1, 2});

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
  txn.mutable_internal()->set_type(TransactionType::SINGLE_HOME);

  SendBatch(100, {txn}, {0, 1, 2});

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
  txn.mutable_internal()->set_type(TransactionType::SINGLE_HOME);

  SendBatch(100, {txn}, {0, 1, 2});

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
  txn.mutable_internal()->set_type(TransactionType::SINGLE_HOME);

  SendBatch(100, {txn}, {0, 1, 2});

  auto output_txn = ReceiveMultipleAndMerge(0, 3);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.read_set_size(), 3);
  ASSERT_EQ(output_txn.read_set().at("D"), "valueD");
  ASSERT_EQ(output_txn.read_set().at("E"), "valueE");
  ASSERT_EQ(output_txn.read_set().at("F"), "valueF");
  ASSERT_EQ(output_txn.write_set_size(), 0);
}