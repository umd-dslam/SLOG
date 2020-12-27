#include <gmock/gmock.h>

#include <algorithm>

#include "common/transaction_holder.h"
#include "common/proto_utils.h"
#include "test/test_utils.h"

using namespace std;
using namespace slog;

using ::testing::ElementsAre;
using ::testing::UnorderedElementsAre;

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

  TransactionHolder MakeHolder(const ConfigurationPtr& config, Transaction* txn) {
    ReusableRequest req(&pool);
    req.get()->mutable_forward_txn()->set_allocated_txn(txn);
    return TransactionHolder{config, move(req)};
  }

  MessagePool<internal::Request> pool;
  ConfigVec configs;
};

TEST_F(TransactionHolderTest, TxnReplicaId) {
  auto txn = MakeTransaction(
      {"A"}, /* read_set */
      {"D"},  /* write_set */
      "", /* code */
      {{"A", {1, 3}}, {"D", {1,0}}}); /* metadata */
  txn->mutable_internal()->set_id(100);

  auto holder = MakeHolder(configs[0], txn);
  ASSERT_EQ(holder.transaction_id_replica_id(), make_pair(100u, 1u));
}

TEST_F(TransactionHolderTest, keys_in_partition) {
  auto txn = MakeTransaction(
      {"A"}, /* read_set */
      {"D"},  /* write_set */
      "", /* code */
      {{"A", {1, 3}}, {"D", {1,0}}}); /* metadata */
  txn->mutable_internal()->set_id(100);

  auto txn2 = new Transaction(*txn);

  auto holder1 = MakeHolder(configs[0], txn);
  ASSERT_THAT(holder1.keys_in_partition(),
    ElementsAre(make_pair("A", LockMode::READ), make_pair("D", LockMode::WRITE)));

  auto holder2 = MakeHolder(configs[1], txn2);
  ASSERT_TRUE(holder2.keys_in_partition().empty());
}

TEST_F(TransactionHolderTest, num_involved_partitions) {
  for (uint32_t i = 0; i < NUM_MACHINES; i++) {
    auto txn = MakeTransaction(
      {"A"}, /* read_set */
      {"B", "D"},  /* write_set */
      "", /* code */
      {{"A", {1, 3}}, {"B", {1,0}}, {"D", {0, 0}}}); /* metadata */
    txn->mutable_internal()->set_id(100);

    auto holder = MakeHolder(configs[i], txn);
    ASSERT_EQ(holder.num_involved_partitions(), 2U);
  }
}

TEST_F(TransactionHolderTest, active_partitions) {
  for (uint32_t i = 0; i < NUM_MACHINES; i++) {
    auto txn = MakeTransaction(
      {"A"}, /* read_set */
      {"B", "D"},  /* write_set */
      "", /* code */
      {{"A", {1, 3}}, {"B", {1,0}}, {"D", {0, 0}}}); /* metadata */
    txn->mutable_internal()->set_id(100);

    auto holder = MakeHolder(configs[i], txn);
    const auto& active_partitions = holder.active_partitions();
    ASSERT_EQ(active_partitions.size(), 2U);
    ASSERT_THAT(active_partitions, UnorderedElementsAre(0U, 2U));
  }
}

TEST_F(TransactionHolderTest, involved_replicas) {
  for (uint32_t i = 0; i < NUM_MACHINES; i++) {
    auto txn = MakeTransaction(
      {"A"}, /* read_set */
      {"B", "D"},  /* write_set */
      "", /* code */
      {{"A", {1, 3}}, {"B", {1,0}}, {"D", {0, 0}}}); /* metadata */
    txn->mutable_internal()->set_id(100);

    auto holder = MakeHolder(configs[i], txn);
    const auto& involved_replicas = holder.involved_replicas();
    ASSERT_EQ(involved_replicas.size(), 2U);
    ASSERT_THAT(involved_replicas, UnorderedElementsAre(0U, 1U));
  }
}