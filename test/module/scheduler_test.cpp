#include <gtest/gtest.h>

#include <vector>

#include "common/proto_utils.h"
#include "test/test_utils.h"

using namespace std;
using namespace slog;

internal::Request MakeBatch(BatchId batch_id, const vector<Transaction*>& txns, TransactionType batch_type) {
  internal::Request req;
  auto batch = req.mutable_forward_batch()->mutable_batch_data();
  batch->set_id(batch_id);
  batch->set_transaction_type(batch_type);
  for (auto txn : txns) {
    batch->mutable_transactions()->AddAllocated(txn);
  }
  return req;
}

internal::Request MakeBatchOrder(uint32_t slot, uint32_t batch_id) {
  internal::Request req;
  req.mutable_forward_batch()->mutable_batch_order()->set_slot(slot);
  req.mutable_forward_batch()->mutable_batch_order()->set_batch_id(batch_id);
  return req;
}

class SchedulerTest : public ::testing::Test {
 protected:
  static const size_t kNumMachines = 6;
  static const uint32_t kNumReplicas = 2;
  static const uint32_t kNumPartitions = 3;

  void SetUp() {
    ConfigVec configs = MakeTestConfigurations("scheduler", kNumReplicas, kNumPartitions);

    for (size_t i = 0; i < kNumMachines; i++) {
      test_slogs[i] = make_unique<TestSlog>(configs[i]);
      test_slogs[i]->AddScheduler();
      sender[i] = test_slogs[i]->NewSender();
      test_slogs[i]->AddOutputChannel(kServerChannel);
    }
    // Relica 0
    test_slogs[0]->Data("A", {"valueA", 0, 1});
    test_slogs[0]->Data("D", {"valueD", 0, 1});
    test_slogs[0]->Data("Y", {"valueY", 1, 1});

    test_slogs[1]->Data("C", {"valueC", 0, 1});
    test_slogs[1]->Data("F", {"valueF", 0, 1});
    test_slogs[1]->Data("X", {"valueX", 1, 1});

    test_slogs[2]->Data("B", {"valueB", 0, 1});
    test_slogs[2]->Data("E", {"valueE", 0, 1});
    test_slogs[2]->Data("Z", {"valueZ", 1, 1});

    // Replica 1
    test_slogs[3]->Data("A", {"valueA", 0, 1});
    test_slogs[3]->Data("D", {"valueD", 0, 1});
    test_slogs[3]->Data("Y", {"valueY", 1, 1});

    test_slogs[4]->Data("C", {"valueC", 0, 1});
    test_slogs[4]->Data("F", {"valueF", 0, 1});
    test_slogs[4]->Data("X", {"valueX", 1, 1});

    test_slogs[5]->Data("B", {"valueB", 0, 1});
    test_slogs[5]->Data("E", {"valueE", 0, 1});
    test_slogs[5]->Data("Z", {"valueZ", 1, 1});

    for (const auto& test_slog : test_slogs) {
      test_slog->StartInNewThreads();
    }
  }

  void SendTransaction(Transaction* txn) {
    CHECK(txn != nullptr);
    for (auto p : txn->internal().involved_partitions()) {
      auto new_txn = GeneratePartitionedTxn(test_slogs[0]->config(), *txn, p);
      if (new_txn != nullptr) {
        internal::Envelope env;
        env.mutable_request()->mutable_forward_txn()->set_allocated_txn(new_txn);
        // This message is sent from machine 0:0, so it will be queued up in queue 0.
        // 'p' just happens to be the same as machine id here.
        sender[0]->Send(env, p, kSchedulerChannel);
      }
    }
  }

  Transaction ReceiveMultipleAndMerge(uint32_t receiver, uint32_t num_partitions) {
    Transaction txn;
    bool first_time = true;
    for (uint32_t i = 0; i < num_partitions; i++) {
      auto req_env = test_slogs[receiver]->ReceiveFromOutputChannel(kServerChannel);
      CHECK(req_env != nullptr);
      CHECK_EQ(req_env->request().type_case(), internal::Request::kCompletedSubtxn);
      auto completed_subtxn = req_env->request().completed_subtxn();
      auto sub_txn = completed_subtxn.txn();
      CHECK_EQ(sub_txn.internal().involved_partitions_size(), num_partitions);

      if (first_time) {
        txn = sub_txn;
      } else {
        MergeTransaction(txn, sub_txn);
      }

      first_time = false;
    }
    return txn;
  }

  MachineId MakeMachineId(int replica, int partition) { return replica * kNumPartitions + partition; }

  unique_ptr<TestSlog> test_slogs[kNumMachines];
  unique_ptr<Sender> sender[kNumMachines];
};

TEST_F(SchedulerTest, SinglePartitionTransaction) {
  auto txn = MakeTestTransaction(test_slogs[0]->config(), 1000,
                                 {{"A", KeyType::READ, {{0, 1}}}, {"D", KeyType::WRITE, {{0, 1}}}},
                                 "GET A     \n"
                                 "SET D newD\n",
                                 MakeMachineId(0, 1));

  SendTransaction(txn);

  auto output_txn = ReceiveMultipleAndMerge(1, 1);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.keys_size(), 2);
  ASSERT_EQ(output_txn.keys().at("A").type(), KeyType::READ);
  ASSERT_EQ(output_txn.keys().at("A").value(), "valueA");
  ASSERT_EQ(output_txn.keys().at("D").type(), KeyType::WRITE);
  ASSERT_EQ(output_txn.keys().at("D").value(), "valueD");
  ASSERT_EQ(output_txn.keys().at("D").new_value(), "newD");
}

TEST_F(SchedulerTest, MultiPartitionTransaction1Active1Passive) {
  auto txn = MakeTestTransaction(test_slogs[0]->config(), 1000,
                                 {{"A", KeyType::READ, {{0, 1}}}, {"C", KeyType::WRITE, {{0, 1}}}}, "COPY A C");

  SendTransaction(txn);

  auto output_txn = ReceiveMultipleAndMerge(0, 2);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.keys_size(), 2);
  ASSERT_EQ(output_txn.keys().at("A").type(), KeyType::READ);
  ASSERT_EQ(output_txn.keys().at("A").value(), "valueA");
  ASSERT_EQ(output_txn.keys().at("C").type(), KeyType::WRITE);
  ASSERT_EQ(output_txn.keys().at("C").value(), "valueC");
  ASSERT_EQ(output_txn.keys().at("C").new_value(), "valueA");
}

TEST_F(SchedulerTest, MultiPartitionTransactionMutualWait2Partitions) {
  auto txn = MakeTestTransaction(test_slogs[0]->config(), 1000,
                                 {{"B", KeyType::WRITE, {{0, 1}}}, {"C", KeyType::WRITE, {{0, 1}}}},
                                 "COPY C B\n"
                                 "COPY B C\n");

  SendTransaction(txn);

  auto output_txn = ReceiveMultipleAndMerge(0, 2);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.keys_size(), 2);
  ASSERT_EQ(output_txn.keys().at("B").type(), KeyType::WRITE);
  ASSERT_EQ(output_txn.keys().at("B").value(), "valueB");
  ASSERT_EQ(output_txn.keys().at("B").new_value(), "valueC");
  ASSERT_EQ(output_txn.keys().at("C").type(), KeyType::WRITE);
  ASSERT_EQ(output_txn.keys().at("C").value(), "valueC");
  ASSERT_EQ(output_txn.keys().at("C").new_value(), "valueB");
}

TEST_F(SchedulerTest, MultiPartitionTransactionWriteOnly) {
  auto txn = MakeTestTransaction(
      test_slogs[0]->config(), 1000,
      {{"A", KeyType::WRITE, {{0, 1}}}, {"B", KeyType::WRITE, {{0, 1}}}, {"C", KeyType::WRITE, {{0, 1}}}},
      "SET A newA\n"
      "SET B newB\n"
      "SET C newC\n");

  SendTransaction(txn);

  auto output_txn = ReceiveMultipleAndMerge(0, 3);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.keys_size(), 3);
  ASSERT_EQ(output_txn.keys().at("A").type(), KeyType::WRITE);
  ASSERT_EQ(output_txn.keys().at("A").value(), "valueA");
  ASSERT_EQ(output_txn.keys().at("A").new_value(), "newA");
  ASSERT_EQ(output_txn.keys().at("B").type(), KeyType::WRITE);
  ASSERT_EQ(output_txn.keys().at("B").value(), "valueB");
  ASSERT_EQ(output_txn.keys().at("B").new_value(), "newB");
  ASSERT_EQ(output_txn.keys().at("C").type(), KeyType::WRITE);
  ASSERT_EQ(output_txn.keys().at("C").value(), "valueC");
  ASSERT_EQ(output_txn.keys().at("C").new_value(), "newC");
}

TEST_F(SchedulerTest, MultiPartitionTransactionReadOnly) {
  auto txn = MakeTestTransaction(
      test_slogs[0]->config(), 1000,
      {{"D", KeyType::READ, {{0, 1}}}, {"E", KeyType::READ, {{0, 1}}}, {"F", KeyType::READ, {{0, 1}}}},
      "GET D\n"
      "GET E\n"
      "GET F\n");

  SendTransaction(txn);

  auto output_txn = ReceiveMultipleAndMerge(0, 3);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.keys_size(), 3);
  ASSERT_EQ(output_txn.keys().at("D").type(), KeyType::READ);
  ASSERT_EQ(output_txn.keys().at("D").value(), "valueD");
  ASSERT_EQ(output_txn.keys().at("E").type(), KeyType::READ);
  ASSERT_EQ(output_txn.keys().at("E").value(), "valueE");
  ASSERT_EQ(output_txn.keys().at("F").type(), KeyType::READ);
  ASSERT_EQ(output_txn.keys().at("F").value(), "valueF");
}

TEST_F(SchedulerTest, SimpleMultiHomeBatch) {
  auto txn = MakeTestTransaction(test_slogs[0]->config(), 1000,
                                 {{"A", KeyType::READ, {{0, 1}}},
                                  {"X", KeyType::READ, {{1, 1}}},
                                  {"C", KeyType::READ, {{0, 1}}},
                                  {"B", KeyType::WRITE, {{0, 1}}},
                                  {"Y", KeyType::WRITE, {{1, 1}}},
                                  {"Z", KeyType::WRITE, {{1, 1}}}},
                                 "GET A\n"
                                 "GET X\n"
                                 "GET C\n"
                                 "SET B newB\n"
                                 "SET Y newY\n"
                                 "SET Z newZ\n");

  auto lo_txn_0 = GenerateLockOnlyTxn(txn, 0);
  auto lo_txn_1 = GenerateLockOnlyTxn(txn, 1);

  delete txn;

  SendTransaction(lo_txn_0);
  SendTransaction(lo_txn_1);

  auto output_txn = ReceiveMultipleAndMerge(0, 3);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.keys_size(), 6);
  ASSERT_EQ(output_txn.keys().at("A").type(), KeyType::READ);
  ASSERT_EQ(output_txn.keys().at("A").value(), "valueA");
  ASSERT_EQ(output_txn.keys().at("X").type(), KeyType::READ);
  ASSERT_EQ(output_txn.keys().at("X").value(), "valueX");
  ASSERT_EQ(output_txn.keys().at("C").type(), KeyType::READ);
  ASSERT_EQ(output_txn.keys().at("C").value(), "valueC");
  ASSERT_EQ(output_txn.keys().at("B").type(), KeyType::WRITE);
  ASSERT_EQ(output_txn.keys().at("B").value(), "valueB");
  ASSERT_EQ(output_txn.keys().at("B").new_value(), "newB");
  ASSERT_EQ(output_txn.keys().at("Y").type(), KeyType::WRITE);
  ASSERT_EQ(output_txn.keys().at("Y").value(), "valueY");
  ASSERT_EQ(output_txn.keys().at("Y").new_value(), "newY");
  ASSERT_EQ(output_txn.keys().at("Z").type(), KeyType::WRITE);
  ASSERT_EQ(output_txn.keys().at("Z").value(), "valueZ");
  ASSERT_EQ(output_txn.keys().at("Z").new_value(), "newZ");
}

TEST_F(SchedulerTest, SinglePartitionTransactionValidateMasters) {
  auto txn = MakeTestTransaction(test_slogs[0]->config(), 1000,
                                 {{"A", KeyType::READ, {{0, 1}}}, {"D", KeyType::WRITE, {{0, 1}}}},
                                 "GET A     \n"
                                 "SET D newD\n",
                                 MakeMachineId(0, 1));

  SendTransaction(txn);

  auto output_txn = ReceiveMultipleAndMerge(1, 1);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.keys_size(), 2);
  ASSERT_EQ(output_txn.keys().at("A").type(), KeyType::READ);
  ASSERT_EQ(output_txn.keys().at("A").value(), "valueA");
  ASSERT_EQ(output_txn.keys().at("D").type(), KeyType::WRITE);
  ASSERT_EQ(output_txn.keys().at("D").value(), "valueD");
  ASSERT_EQ(output_txn.keys().at("D").new_value(), "newD");
}

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
TEST_F(SchedulerTest, SinglePartitionTransactionProcessRemaster) {
  auto remaster_txn = MakeTestTransaction(test_slogs[0]->config(), 2000, {{"A", KeyType::WRITE, {{0, 1}}}},
                                          1 /* remaster */, MakeMachineId(0, 1));
  auto txn = MakeTestTransaction(test_slogs[0]->config(), 1000, {{"A", KeyType::READ, {{1, 2}}}}, "GET A",
                                 MakeMachineId(0, 0));

  SendTransaction(txn);
  SendTransaction(remaster_txn);

  auto output_remaster_txn = ReceiveMultipleAndMerge(1, 1);
  LOG(INFO) << output_remaster_txn;
  ASSERT_EQ(output_remaster_txn.internal().id(), 2000);
  ASSERT_EQ(output_remaster_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_remaster_txn.remaster().new_master(), 1);

  auto output_txn = ReceiveMultipleAndMerge(0, 1);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.internal().id(), 1000);
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.keys_size(), 1);
  ASSERT_EQ(output_txn.keys().at("A").type(), KeyType::READ);
  ASSERT_EQ(output_txn.keys().at("A").value(), "valueA");
}
#endif /* defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY) */

#ifdef REMASTER_PROTOCOL_COUNTERLESS
TEST_F(SchedulerTest, SinglePartitionTransactionProcessRemaster) {
  auto remaster_txn = MakeTestTransaction(test_slogs[0]->config(), 1000, {{"A", KeyType::WRITE, 0}}, 1, /* new master */
                                          MakeMachineId(0, 1));

  auto remaster_txn_lo_0 = GenerateLockOnlyTxn(remaster_txn, 0);
  auto remaster_txn_lo_1 = GenerateLockOnlyTxn(remaster_txn, 1);

  delete remaster_txn;

  auto txn =
      MakeTestTransaction(test_slogs[0]->config(), 2000, {{"A", KeyType::READ, 1}}, "GET A", MakeMachineId(0, 0));

  SendTransaction(remaster_txn_lo_1);
  SendTransaction(remaster_txn_lo_0);
  SendTransaction(txn);

  auto output_remaster_txn = ReceiveMultipleAndMerge(1, 1);
  LOG(INFO) << output_remaster_txn;
  ASSERT_EQ(output_remaster_txn.internal().id(), 1000U);
  ASSERT_EQ(output_remaster_txn.status(), TransactionStatus::COMMITTED);

  auto output_txn = ReceiveMultipleAndMerge(0, 1);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.internal().id(), 2000U);
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.keys_size(), 1);
  ASSERT_EQ(output_txn.keys().at("A").type(), KeyType::READ);
  ASSERT_EQ(output_txn.keys().at("A").value(), "valueA");
}
#endif /* REMASTERING_PROTOCOL_COUNTERLESS */

TEST_F(SchedulerTest, AbortSingleHomeSinglePartition) {
  auto txn = MakeTestTransaction(test_slogs[0]->config(), 1000, {{"A", KeyType::READ, {{1, 0}}}}, "GET A",
                                 MakeMachineId(0, 1));

  SendTransaction(txn);

  auto output_txn = ReceiveMultipleAndMerge(1, 1);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::ABORTED);
}

TEST_F(SchedulerTest, AbortMultiHomeSinglePartition) {
  auto txn = MakeTestTransaction(
      test_slogs[0]->config(), 1000,
      {{"A", KeyType::READ, {{0, 1}}}, {"D", KeyType::READ, {{1, 0}}}, {"Y", KeyType::WRITE, {{1, 1}}}});

  auto lo_txn_0 = GenerateLockOnlyTxn(txn, 0);
  auto lo_txn_1 = GenerateLockOnlyTxn(txn, 1);

  delete txn;

  SendTransaction(lo_txn_0);
  SendTransaction(lo_txn_1);

  auto output_txn = ReceiveMultipleAndMerge(0, 1);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::ABORTED);
}

TEST_F(SchedulerTest, AbortSingleHomeMultiPartition) {
  auto txn = MakeTestTransaction(test_slogs[0]->config(), 1000,
                                 {{"A", KeyType::READ, {{1, 0}}}, {"X", KeyType::WRITE, {{1, 1}}}},
                                 "GET A     \n"
                                 "SET X newC    \n",
                                 MakeMachineId(0, 1));

  SendTransaction(txn);

  auto output_txn = ReceiveMultipleAndMerge(1, 2);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::ABORTED);
}

TEST_F(SchedulerTest, AbortSingleHomeMultiPartition2Active) {
  auto txn = MakeTestTransaction(
      test_slogs[0]->config(), 1000,
      {{"Y", KeyType::READ, {{1, 1}}}, {"C", KeyType::WRITE, {{1, 0}}}, {"B", KeyType::WRITE, {{1, 0}}}},
      "GET Y     \n"
      "SET C newC    \n"
      "SET B newB    \n",
      MakeMachineId(0, 1));

  SendTransaction(txn);

  auto output_txn = ReceiveMultipleAndMerge(1, 3);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::ABORTED);
}

TEST_F(SchedulerTest, AbortMultiHomeMultiPartition2Active) {
  // D and X have outdated master information
  auto txn = MakeTestTransaction(test_slogs[0]->config(), 1000,
                                 {{"A", KeyType::READ, {{0, 1}}},
                                  {"D", KeyType::READ, {{1, 0}}},
                                  {"Y", KeyType::WRITE, {{1, 1}}},
                                  {"X", KeyType::WRITE, {{0, 0}}}},
                                 "");

  auto lo_txn_0 = GenerateLockOnlyTxn(txn, 0);
  auto lo_txn_1 = GenerateLockOnlyTxn(txn, 1);

  delete txn;

  SendTransaction(lo_txn_0);
  SendTransaction(lo_txn_1);

  auto output_txn = ReceiveMultipleAndMerge(0, 2);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::ABORTED);
}

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  google::InstallFailureSignalHandler();
  return RUN_ALL_TESTS();
}