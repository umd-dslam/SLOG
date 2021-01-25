#include "common/txn_holder.h"

#include <gmock/gmock.h>

#include <algorithm>

#include "common/proto_utils.h"
#include "test/test_utils.h"

using namespace std;
using namespace slog;

using ::testing::UnorderedElementsAre;

class TxnHolderTest : public ::testing::Test {
 protected:
  static const size_t NUM_MACHINES = 6;
  void SetUp() {
    configs = MakeTestConfigurations("txn_holder", 2 /* num_replicas */, 3 /* num_partitions */);

    /*
    Partition 0: A, D, Y
    Partition 1: C, F, X
    Partition 2: B, E, Z
    */
  }

  ConfigVec configs;
};

TEST_F(TxnHolderTest, keys_in_partition) {
  auto txn = MakeTransaction({"A"},                           /* read_set */
                             {"D"},                           /* write_set */
                             "",                              /* code */
                             {{"A", {1, 3}}, {"D", {1, 0}}}); /* metadata */
  txn->mutable_internal()->set_id(100);

  auto txn2 = new Transaction(*txn);

  TxnHolder holder1(configs[0], txn);
  ASSERT_THAT(holder1.any_lock_only_txn()->keys, UnorderedElementsAre(LockOnlyTxn::KeyInfo("A", LockMode::READ, 3),
                                                                      LockOnlyTxn::KeyInfo("D", LockMode::WRITE, 0)));
  ASSERT_EQ(holder1.any_lock_only_txn()->master, 1);

  TxnHolder holder2(configs[1], txn2);
  ASSERT_TRUE(holder2.any_lock_only_txn()->keys.empty());
}

TEST_F(TxnHolderTest, active_partitions) {
  for (uint32_t i = 0; i < NUM_MACHINES; i++) {
    auto txn = MakeTransaction({"A"},                                          /* read_set */
                               {"B", "D"},                                     /* write_set */
                               "",                                             /* code */
                               {{"A", {1, 3}}, {"B", {1, 0}}, {"D", {0, 0}}}); /* metadata */
    txn->mutable_internal()->set_id(100);

    TxnHolder holder(configs[i], txn);
    const auto& active_partitions = holder.active_partitions();
    ASSERT_EQ(active_partitions.size(), 2U);
    ASSERT_THAT(active_partitions, UnorderedElementsAre(0U, 2U));
  }
}