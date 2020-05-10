#include <gmock/gmock.h>

#include "common/transaction_holder.h"
#include "common/proto_utils.h"
#include "common/test_utils.h"

using namespace std;
using namespace slog;

using ::testing::ElementsAre;

class TransactionHolderTest : public ::testing::Test {
protected:
  static const size_t NUM_MACHINES = 6;
  void SetUp() {
    configs = MakeTestConfigurations(
        "txn_holder",
        2 /* num_replicas */,
        3 /* num_partitions */);

    /* 
    Partition 0: A, D, Y
    Partition 1: C, F, X
    Partition 2: B, E, Z
    */
  }

  ConfigVec configs;
};

TEST_F(TransactionHolderTest, TxnReplicaId) {
  auto txn = MakeTransaction(
      {"A"}, /* read_set */
      {"D"},  /* write_set */
      "", /* code */
      {{"A", {1, 3}}, {"D", {1,0}}}); /* metadata */
  txn->mutable_internal()->set_id(100);

  auto holder = TransactionHolder(configs[0], txn);
  ASSERT_EQ(holder.GetTransactionIdReplicaIdPair(), make_pair(100u, 1u));
}

TEST_F(TransactionHolderTest, KeysInPartition) {
  auto txn = MakeTransaction(
      {"A"}, /* read_set */
      {"D"},  /* write_set */
      "", /* code */
      {{"A", {1, 3}}, {"D", {1,0}}}); /* metadata */
  txn->mutable_internal()->set_id(100);

  auto holder1 = TransactionHolder(configs[0], txn);
  ASSERT_THAT(holder1.KeysInPartition(),
    ElementsAre(make_pair("A", LockMode::READ), make_pair("D", LockMode::WRITE)));

  auto holder2 = TransactionHolder(configs[1], holder1.ReleaseTransaction());
  ASSERT_TRUE(holder2.KeysInPartition().empty());
}

TEST_F(TransactionHolderTest, InvolvedPartitions) {
  for (uint32_t i = 0; i < NUM_MACHINES; i++) {
    auto txn = MakeTransaction(
      {"A"}, /* read_set */
      {"B", "D"},  /* write_set */
      "", /* code */
      {{"A", {1, 3}}, {"B", {1,0}}, {"A", {1, 0}}}); /* metadata */
    txn->mutable_internal()->set_id(100);

    auto holder = TransactionHolder(configs[i], txn);
    auto partitions = holder.InvolvedPartitions();
    ASSERT_EQ(partitions.size(), 2);
    ASSERT_TRUE(partitions.count(0));
    ASSERT_TRUE(partitions.count(2));
  }
}