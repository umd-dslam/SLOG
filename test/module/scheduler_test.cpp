#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "common/test_utils.h"
#include "common/proto_utils.h"

using namespace std;
using namespace slog;

internal::Request MakeForwardBatchRequest(
    BatchId batch_id, const vector<Transaction>& txns) {
  internal::Request req;
  auto batch = req.mutable_forward_batch()->mutable_batch();
  batch->set_id(batch_id);
  for (auto txn : txns) {
    batch->mutable_transactions()->Add(move(txn));
  }
  return req;
}

internal::Request MakePaxosOrder(uint32_t slot, uint32_t value) {
  internal::Request req;
  req.mutable_paxos_order()->set_slot(slot);
  req.mutable_paxos_order()->set_value(value);
  return req;
}

class SchedulerTest : public ::testing::Test {
protected:
  static const size_t NUM_MACHINES = 3;

  void SetUp() {
    ConfigVec configs = MakeTestConfigurations(
        "scheduler", 1 /* num_replicas */, 3 /* num_partitions */);

    for (size_t i = 0; i < NUM_MACHINES; i++) {
      test_slogs_[i] = make_unique<TestSlog>(configs[i]);
      test_slogs_[i]->AddScheduler();
      input_[i] = test_slogs_[i]->AddChannel(SEQUENCER_CHANNEL);
      output_[i] = test_slogs_[i]->AddChannel(SERVER_CHANNEL);
    }
    test_slogs_[0]->Data("A", {"valueA", 0, 0});
    test_slogs_[0]->Data("D", {"valueD", 0, 0});
    test_slogs_[1]->Data("C", {"valueC", 0, 0});
    test_slogs_[1]->Data("F", {"valueF", 0, 0});
    test_slogs_[2]->Data("B", {"valueB", 0, 0});
    test_slogs_[2]->Data("E", {"valueE", 0, 0});

    for (const auto& test_slog : test_slogs_) {
      test_slog->StartInNewThreads();
    }
  }

  unique_ptr<TestSlog> test_slogs_[NUM_MACHINES];
  unique_ptr<Channel> input_[NUM_MACHINES];
  unique_ptr<Channel> output_[NUM_MACHINES];
};

TEST_F(SchedulerTest, SinglePartitionTransaction) {
  auto txn = MakeTransaction(
      {"C"}, /* read_set */
      {"F"},  /* write_set */
      "GET C    \n"
      "SET F newF", /* code */
      {},
      MakeMachineIdProto("0:1") /* coordinating server */);

  MMessage msg;
  msg.Set(MM_PROTO, MakeForwardBatchRequest(100, {txn}));
  msg.Set(MM_TO_CHANNEL, SCHEDULER_CHANNEL);
  input_[1]->Send(msg);

  msg.Clear();
  msg.Set(MM_PROTO, MakePaxosOrder(0, 1 /* partition 1 */));
  msg.Set(MM_TO_CHANNEL, SCHEDULER_CHANNEL);
  input_[1]->Send(msg);

  output_[1]->Receive(msg);
  internal::Request req;
  ASSERT_TRUE(msg.GetProto(req));
  ASSERT_EQ(req.type_case(), internal::Request::kForwardTxn);
  auto output_txn = req.forward_txn().txn();
  ASSERT_EQ(output_txn.read_set_size(), 1);
  ASSERT_EQ(output_txn.read_set().at("C"), "valueC");
  ASSERT_EQ(output_txn.write_set_size(), 1);
  ASSERT_EQ(output_txn.write_set().at("F"), "newF");
}
