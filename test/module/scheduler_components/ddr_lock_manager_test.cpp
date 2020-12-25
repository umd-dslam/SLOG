#include <gtest/gtest.h>

#include "test/test_utils.h"
#include "common/proto_utils.h"
#include "module/scheduler_components/ddr_lock_manager.h"

using namespace std;
using namespace slog;

class DDRLockManagerTest : public ::testing::Test {
protected:
  DDRLockManager lock_manager;
  MessagePool<internal::Request> pool;

  TransactionHolder MakeHolder(const ConfigurationPtr& config, Transaction* txn) {
    ReusableRequest req(&pool);
    req.get()->mutable_forward_txn()->set_allocated_txn(txn);
    return TransactionHolder{config, move(req)};
  }
};

TEST_F(DDRLockManagerTest, GetAllLocksOnFirstTry) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn = FillMetadata(MakeTransaction(
    {"readA", "readB"},
    {"writeC"}));
  txn->mutable_internal()->set_num_masters(1);
  TransactionHolder holder = MakeHolder(configs[0], txn);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder), AcquireLocksResult::ACQUIRED);
  auto result = lock_manager.ReleaseLocks(holder);
  ASSERT_TRUE(result.empty());
}

TEST_F(DDRLockManagerTest, ReadLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = FillMetadata(MakeTransaction({"readA", "readB"}, {}));
  txn1->mutable_internal()->set_id(100);
  txn1->mutable_internal()->set_num_masters(1);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);

  auto txn2 = FillMetadata(MakeTransaction({"readB", "readC"}, {}));
  txn2->mutable_internal()->set_id(200);
  txn2->mutable_internal()->set_num_masters(1);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder2), AcquireLocksResult::ACQUIRED);
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder1).empty());
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder2).empty());
}

TEST_F(DDRLockManagerTest, WriteLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = FillMetadata(MakeTransaction({}, {"writeA", "writeB"}));
  txn1->mutable_internal()->set_id(100);
  txn1->mutable_internal()->set_num_masters(1);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);

  auto txn2 = FillMetadata(MakeTransaction({"readA"}, {"writeA"}));
  txn2->mutable_internal()->set_id(200);
  txn2->mutable_internal()->set_num_masters(1);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder2), AcquireLocksResult::WAITING);
  // The blocked txn becomes ready
  ASSERT_EQ(lock_manager.ReleaseLocks(holder1).size(), 1U);
  // Make sure the lock is already held by txn2
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder1), AcquireLocksResult::WAITING);
}

TEST_F(DDRLockManagerTest, ReleaseLocksAndReturnMultipleNewLockHolders) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = FillMetadata(MakeTransaction({"A"}, {"B", "C"}));
  txn1->mutable_internal()->set_id(100);
  txn1->mutable_internal()->set_num_masters(1);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);
  auto txn2 = FillMetadata(MakeTransaction({"B"}, {"A"}));
  txn2->mutable_internal()->set_id(200);
  txn2->mutable_internal()->set_num_masters(1);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);
  auto txn3 = FillMetadata(MakeTransaction({}, {"A"}));
  txn3->mutable_internal()->set_id(300);
  txn3->mutable_internal()->set_num_masters(1);
  TransactionHolder holder3 = MakeHolder(configs[0], txn3);
  auto txn4 = FillMetadata(MakeTransaction({"C"}, {}));
  txn4->mutable_internal()->set_id(400);
  txn4->mutable_internal()->set_num_masters(1);
  TransactionHolder holder4 = MakeHolder(configs[0], txn4);

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder2), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder3), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder4), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(holder1);
  ASSERT_EQ(result.size(), 2U);
  ASSERT_TRUE(find(result.begin(), result.end(), 200) != result.end());
  ASSERT_TRUE(find(result.begin(), result.end(), 400) != result.end());

  ASSERT_TRUE(lock_manager.ReleaseLocks(holder4).empty());

  result = lock_manager.ReleaseLocks(holder2);
  ASSERT_EQ(result.size(), 1U);
  ASSERT_EQ(result[0], 300);

  ASSERT_TRUE(lock_manager.ReleaseLocks(holder3).empty());
}

TEST_F(DDRLockManagerTest, PartiallyAcquiredLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = FillMetadata(MakeTransaction({"A"}, {"B", "C"}));
  txn1->mutable_internal()->set_id(100);
  txn1->mutable_internal()->set_num_masters(1);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);
  auto txn2 = FillMetadata(MakeTransaction({"A"}, {"B"}));
  txn2->mutable_internal()->set_id(200);
  txn2->mutable_internal()->set_num_masters(1);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);
  auto txn3 = FillMetadata(MakeTransaction({}, {"A", "C"}));
  txn3->mutable_internal()->set_id(300);
  txn3->mutable_internal()->set_num_masters(1);
  TransactionHolder holder3 = MakeHolder(configs[0], txn3);

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder2), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder3), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(holder1);
  ASSERT_EQ(result.size(), 1U);
  ASSERT_TRUE(find(result.begin(), result.end(), 200) != result.end());

  result = lock_manager.ReleaseLocks(holder2);
  ASSERT_EQ(result.size(), 1U);
  ASSERT_TRUE(find(result.begin(), result.end(), 300) != result.end());
}

TEST_F(DDRLockManagerTest, PrioritizeWriteLock) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = FillMetadata(MakeTransaction({"A"}, {"A"}));
  txn1->mutable_internal()->set_id(100);
  txn1->mutable_internal()->set_num_masters(1);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);
  auto txn2 = FillMetadata(MakeTransaction({"A"}, {}));
  txn2->mutable_internal()->set_id(200);
  txn2->mutable_internal()->set_num_masters(1);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);
 
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder2), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(holder1);
  ASSERT_EQ(result.size(), 1U);
  ASSERT_TRUE(find(result.begin(), result.end(), 200) != result.end());
}

TEST_F(DDRLockManagerTest, AcquireLocksWithLockOnlyTxn1) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = FillMetadata(MakeTransaction({"A"}, {"B", "C"}));
  txn1->mutable_internal()->set_id(100);
  txn1->mutable_internal()->set_num_masters(1);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);
  auto txn2 = FillMetadata(MakeTransaction({"A"}, {"B"}));
  txn2->mutable_internal()->set_id(200);
  txn2->mutable_internal()->set_num_masters(2);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);
  auto txn2_lockonly1 = FillMetadata(MakeTransaction({}, {"B"}));
  txn2_lockonly1->mutable_internal()->set_id(200);
  TransactionHolder holder2_lockonly1 = MakeHolder(configs[0], txn2_lockonly1);
  auto txn2_lockonly2 = FillMetadata(MakeTransaction({"A"}, {}));
  txn2_lockonly2->mutable_internal()->set_id(200);
  TransactionHolder holder2_lockonly2 = MakeHolder(configs[0], txn2_lockonly2);

  ASSERT_FALSE(lock_manager.AcceptTransaction(holder1));
  ASSERT_FALSE(lock_manager.AcceptTransaction(holder2));
  ASSERT_EQ(lock_manager.AcquireLocks(holder2_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2_lockonly2), AcquireLocksResult::ACQUIRED);

  auto result = lock_manager.ReleaseLocks(holder2);
  ASSERT_EQ(result.size(), 1U);
  ASSERT_TRUE(find(result.begin(), result.end(), 100) != result.end());
}

TEST_F(DDRLockManagerTest, AcquireLocksWithLockOnlyTxn2) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = FillMetadata(MakeTransaction({"A"}, {"B", "C"}));
  txn1->mutable_internal()->set_id(100);
  txn1->mutable_internal()->set_num_masters(1);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);
  auto txn2 = FillMetadata(MakeTransaction({"A"}, {"B"}));
  txn2->mutable_internal()->set_id(200);
  txn2->mutable_internal()->set_num_masters(2);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);
  auto txn2_lockonly1 = FillMetadata(MakeTransaction({}, {"B"}));
  txn2_lockonly1->mutable_internal()->set_id(200);
  TransactionHolder holder2_lockonly1 = MakeHolder(configs[0], txn2_lockonly1);
  auto txn2_lockonly2 = FillMetadata(MakeTransaction({"A"}, {}));
  txn2_lockonly2->mutable_internal()->set_id(200);
  TransactionHolder holder2_lockonly2 = MakeHolder(configs[0], txn2_lockonly2);

  ASSERT_EQ(lock_manager.AcquireLocks(holder2_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2_lockonly2), AcquireLocksResult::WAITING);
  ASSERT_FALSE(lock_manager.AcceptTransaction(holder1));
  ASSERT_TRUE(lock_manager.AcceptTransaction(holder2));

  auto result = lock_manager.ReleaseLocks(holder2);
  ASSERT_EQ(result.size(), 1U);
  ASSERT_TRUE(find(result.begin(), result.end(), 100) != result.end());
}

TEST_F(DDRLockManagerTest, AcquireLocksWithLockOnlyTxnOutOfOrder) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = FillMetadata(MakeTransaction({"A"}, {"B", "C"}));
  txn1->mutable_internal()->set_id(100);
  txn1->mutable_internal()->set_num_masters(1);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);
  auto txn2 = FillMetadata(MakeTransaction({"A"}, {"B"}));
  txn2->mutable_internal()->set_id(200);
  txn2->mutable_internal()->set_num_masters(2);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);
  auto txn2_lockonly1 = FillMetadata(MakeTransaction({}, {"B"}));
  txn2_lockonly1->mutable_internal()->set_id(200);
  TransactionHolder holder2_lockonly1 = MakeHolder(configs[0], txn2_lockonly1);
  auto txn2_lockonly2 = FillMetadata(MakeTransaction({"A"}, {}));
  txn2_lockonly2->mutable_internal()->set_id(200);
  TransactionHolder holder2_lockonly2 = MakeHolder(configs[0], txn2_lockonly2);

  ASSERT_EQ(lock_manager.AcquireLocks(holder2_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_FALSE(lock_manager.AcceptTransaction(holder2));
  ASSERT_EQ(lock_manager.AcquireLocks(holder1), AcquireLocksResult::WAITING);
  ASSERT_FALSE(lock_manager.AcceptTransaction(holder1));
  ASSERT_EQ(lock_manager.AcquireLocks(holder2_lockonly2), AcquireLocksResult::ACQUIRED);

  auto result = lock_manager.ReleaseLocks(holder2);
  ASSERT_EQ(result.size(), 1U);
  ASSERT_TRUE(find(result.begin(), result.end(), 100) != result.end());
}

TEST_F(DDRLockManagerTest, MultiEdgeBetweenTwoTxns) {
  auto configs = MakeTestConfigurations("locking", 1, 1);

  auto txn1 = FillMetadata(MakeTransaction({}, {"A", "B"}));
  txn1->mutable_internal()->set_id(100);
  txn1->mutable_internal()->set_num_masters(1);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);
  auto txn2 = FillMetadata(MakeTransaction({"A", "B"}, {}));
  txn2->mutable_internal()->set_id(200);
  txn2->mutable_internal()->set_num_masters(2);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);
  auto txn2_lockonly1 = FillMetadata(MakeTransaction({"A"}, {}));
  txn2_lockonly1->mutable_internal()->set_id(200);
  TransactionHolder holder2_lockonly1 = MakeHolder(configs[0], txn2_lockonly1);
  auto txn2_lockonly2 = FillMetadata(MakeTransaction({"B"}, {}));
  txn2_lockonly2->mutable_internal()->set_id(200);
  TransactionHolder holder2_lockonly2 = MakeHolder(configs[0], txn2_lockonly2);

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2_lockonly2), AcquireLocksResult::WAITING);
  ASSERT_FALSE(lock_manager.AcceptTransaction(holder2));

  auto result = lock_manager.ReleaseLocks(holder1);
  ASSERT_EQ(result.size(), 1);
  ASSERT_EQ(result[0], 200);

  result = lock_manager.ReleaseLocks(holder2);
  ASSERT_TRUE(result.empty());
}

TEST_F(DDRLockManagerTest, KeyReplicaLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = FillMetadata(MakeTransaction({}, {"writeA", "writeB"}));
  txn1->mutable_internal()->set_id(100);
  txn1->mutable_internal()->set_num_masters(1);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);

  auto txn2 = FillMetadata(MakeTransaction({"readA"}, {"writeA"}), 1);
  txn2->mutable_internal()->set_id(200);
  txn2->mutable_internal()->set_num_masters(1);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder2), AcquireLocksResult::ACQUIRED);
}

TEST_F(DDRLockManagerTest, RemasterTxn) {
  auto configs = MakeTestConfigurations("locking", 2, 1);
  auto txn = FillMetadata(MakeTransaction({}, {"A"}));
  txn->mutable_internal()->set_id(100);
  txn->mutable_remaster()->set_new_master(1);
  txn->mutable_internal()->set_num_masters(2);
  TransactionHolder holder = MakeHolder(configs[0], txn);
  auto txn_lockonly1 = FillMetadata(MakeTransaction({}, {"A"}));
  txn_lockonly1->mutable_internal()->set_id(100);
  txn_lockonly1->mutable_remaster()->set_new_master(1);
  TransactionHolder holder_lockonly1 = MakeHolder(configs[0], txn_lockonly1);
  auto txn_lockonly2 = FillMetadata(MakeTransaction({}, {"A"}));
  txn_lockonly2->mutable_internal()->set_id(100);
  txn_lockonly1->mutable_remaster()->set_new_master(1);
  txn_lockonly1->mutable_remaster()->set_is_new_master_lock_only(true);
  TransactionHolder holder_lockonly2 = MakeHolder(configs[0], txn_lockonly2);

  ASSERT_FALSE(lock_manager.AcceptTransaction(holder));
  ASSERT_EQ(lock_manager.AcquireLocks(holder_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder_lockonly2), AcquireLocksResult::ACQUIRED);

  lock_manager.ReleaseLocks(holder);
  // Check locks have been released
  auto txn2 = FillMetadata(MakeTransaction({}, {"A"}));
  txn2->mutable_internal()->set_num_masters(1);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);

  auto txn3 = FillMetadata(MakeTransaction({}, {"A"}), 1);
  txn3->mutable_internal()->set_num_masters(1);
  TransactionHolder holder3 = MakeHolder(configs[0], txn3);

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder2), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder3), AcquireLocksResult::ACQUIRED);
}

TEST_F(DDRLockManagerTest, EnsureStateIsClean) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = FillMetadata(MakeTransaction({"A"}, {"B", "C"}));
  txn1->mutable_internal()->set_id(100);
  txn1->mutable_internal()->set_num_masters(1);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);
  auto txn2 = FillMetadata(MakeTransaction({"B"}, {"A"}));
  txn2->mutable_internal()->set_id(200);
  txn2->mutable_internal()->set_num_masters(1);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);
  auto txn3 = FillMetadata(MakeTransaction({}, {"C"}));
  txn3->mutable_internal()->set_id(300);
  txn3->mutable_internal()->set_num_masters(1);
  TransactionHolder holder3 = MakeHolder(configs[0], txn3);

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder1), AcquireLocksResult::ACQUIRED);
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder1).empty());

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder2), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder3), AcquireLocksResult::ACQUIRED);
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder2).empty());
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder3).empty());
}


TEST_F(DDRLockManagerTest, LongChain) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = FillMetadata(MakeTransaction({}, {"A"}));
  txn1->mutable_internal()->set_id(100);
  txn1->mutable_internal()->set_num_masters(1);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);
  auto txn2 = FillMetadata(MakeTransaction({"A"}, {}));
  txn2->mutable_internal()->set_id(200);
  txn2->mutable_internal()->set_num_masters(1);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);
  auto txn3 = FillMetadata(MakeTransaction({"A"}, {}));
  txn3->mutable_internal()->set_id(300);
  txn3->mutable_internal()->set_num_masters(1);
  TransactionHolder holder3 = MakeHolder(configs[0], txn3);
  auto txn4 = FillMetadata(MakeTransaction({}, {"A"}));
  txn4->mutable_internal()->set_id(400);
  txn4->mutable_internal()->set_num_masters(1);
  TransactionHolder holder4 = MakeHolder(configs[0], txn4);
  auto txn5 = FillMetadata(MakeTransaction({"A"}, {}));
  txn5->mutable_internal()->set_id(500);
  txn5->mutable_internal()->set_num_masters(1);
  TransactionHolder holder5 = MakeHolder(configs[0], txn5);

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder2), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder3), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder4), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder5), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(holder1);
  ASSERT_EQ(result.size(), 2U);
  ASSERT_TRUE(find(result.begin(), result.end(), 200) != result.end());
  ASSERT_TRUE(find(result.begin(), result.end(), 300) != result.end());

  ASSERT_TRUE(lock_manager.ReleaseLocks(holder2).empty());
  result = lock_manager.ReleaseLocks(holder3);
  ASSERT_EQ(result.size(), 1U);
  ASSERT_EQ(result[0], 400);

  result = lock_manager.ReleaseLocks(holder4);
  ASSERT_EQ(result.size(), 1U);
  ASSERT_EQ(result[0], 500);

  ASSERT_TRUE(lock_manager.ReleaseLocks(holder5).empty());
}