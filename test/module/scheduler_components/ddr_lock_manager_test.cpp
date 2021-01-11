#include "module/scheduler_components/ddr_lock_manager.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common/proto_utils.h"
#include "test/test_utils.h"

using namespace std;
using namespace slog;
using testing::ElementsAre;
using testing::UnorderedElementsAre;

class DDRLockManagerTest : public ::testing::Test {
 protected:
  DDRLockManager lock_manager;
};

TEST_F(DDRLockManagerTest, GetAllLocksOnFirstTry) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn = MakeTxnHolder(configs[0], 100, {"readA", "readB"}, {"writeC"});
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn), AcquireLocksResult::ACQUIRED);
  auto result = lock_manager.ReleaseLocks(txn);
  ASSERT_TRUE(result.empty());
}

TEST_F(DDRLockManagerTest, ReadLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = MakeTxnHolder(configs[0], 100, {"readA", "readB"}, {});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"readB", "readC"}, {});
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn2), AcquireLocksResult::ACQUIRED);
  ASSERT_TRUE(lock_manager.ReleaseLocks(txn1).empty());
  ASSERT_TRUE(lock_manager.ReleaseLocks(txn2).empty());
}

TEST_F(DDRLockManagerTest, WriteLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = MakeTxnHolder(configs[0], 100, {}, {"writeA", "writeB"});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"readA"}, {"writeA"});

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn2), AcquireLocksResult::WAITING);
  // The blocked txn becomes ready
  ASSERT_EQ(lock_manager.ReleaseLocks(txn1).size(), 1U);
  // Make sure the lock is already held by txn2
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn1), AcquireLocksResult::WAITING);
}

TEST_F(DDRLockManagerTest, ReleaseLocksAndReturnMultipleNewLockHolders) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = MakeTxnHolder(configs[0], 100, {"A"}, {"B", "C"});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"B"}, {"A"});
  auto txn3 = MakeTxnHolder(configs[0], 300, {}, {"A"});
  auto txn4 = MakeTxnHolder(configs[0], 400, {"C"}, {});

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn2), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn3), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn4), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(txn1);
  ASSERT_EQ(result.size(), 2U);
  ASSERT_TRUE(find(result.begin(), result.end(), 200) != result.end());
  ASSERT_TRUE(find(result.begin(), result.end(), 400) != result.end());

  ASSERT_TRUE(lock_manager.ReleaseLocks(txn4).empty());

  result = lock_manager.ReleaseLocks(txn2);
  ASSERT_THAT(result, ElementsAre(300));

  ASSERT_TRUE(lock_manager.ReleaseLocks(txn3).empty());
}

TEST_F(DDRLockManagerTest, PartiallyAcquiredLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = MakeTxnHolder(configs[0], 100, {"A"}, {"B", "C"});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"A"}, {"B"});
  auto txn3 = MakeTxnHolder(configs[0], 300, {}, {"A", "C"});

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn2), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn3), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(txn1);
  ASSERT_THAT(result, ElementsAre(200));

  result = lock_manager.ReleaseLocks(txn2);
  ASSERT_THAT(result, ElementsAre(300));
}

TEST_F(DDRLockManagerTest, PrioritizeWriteLock) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = MakeTxnHolder(configs[0], 100, {"A"}, {"A"});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"A"}, {});

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn2), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(txn1);
  ASSERT_THAT(result, ElementsAre(200));
}

TEST_F(DDRLockManagerTest, AcquireLocksWithLockOnlyTxn1) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = MakeTxnHolder(configs[0], 100, {"A"}, {"B", "C"});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"A"}, {"B"});
  auto txn2_lockonly1 = MakeTxnHolder(configs[0], 200, {}, {"B"});
  auto txn2_lockonly2 = MakeTxnHolder(configs[0], 200, {"A"}, {});

  ASSERT_FALSE(lock_manager.AcceptTransaction(txn1));
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn2));
  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly2), AcquireLocksResult::ACQUIRED);

  auto result = lock_manager.ReleaseLocks(txn2);
  ASSERT_THAT(result, ElementsAre(100));
}

TEST_F(DDRLockManagerTest, AcquireLocksWithLockOnlyTxn2) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = MakeTxnHolder(configs[0], 100, {"A"}, {"B", "C"});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"A"}, {"B"});
  auto txn2_lockonly1 = MakeTxnHolder(configs[0], 200, {}, {"B"});
  auto txn2_lockonly2 = MakeTxnHolder(configs[0], 200, {"A"}, {});

  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly2), AcquireLocksResult::WAITING);
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn1));
  ASSERT_TRUE(lock_manager.AcceptTransaction(txn2));

  auto result = lock_manager.ReleaseLocks(txn2);
  ASSERT_THAT(result, ElementsAre(100));
}

TEST_F(DDRLockManagerTest, AcquireLocksWithLockOnlyTxnOutOfOrder) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = MakeTxnHolder(configs[0], 100, {"A"}, {"B", "C"});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"A"}, {"B"});
  auto txn2_lockonly1 = MakeTxnHolder(configs[0], 200, {}, {"B"});
  auto txn2_lockonly2 = MakeTxnHolder(configs[0], 200, {"A"}, {});

  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn2));
  ASSERT_EQ(lock_manager.AcquireLocks(txn1), AcquireLocksResult::WAITING);
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn1));
  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly2), AcquireLocksResult::ACQUIRED);

  auto result = lock_manager.ReleaseLocks(txn2);
  ASSERT_THAT(result, ElementsAre(100));
}

TEST_F(DDRLockManagerTest, MultiEdgeBetweenTwoTxns) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = MakeTxnHolder(configs[0], 100, {}, {"A", "B"});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"A", "B"}, {});
  auto txn2_lockonly1 = MakeTxnHolder(configs[0], 200, {"A"}, {});
  auto txn2_lockonly2 = MakeTxnHolder(configs[0], 200, {"B"}, {});

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly2), AcquireLocksResult::WAITING);
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn2));

  auto result = lock_manager.ReleaseLocks(txn1);
  ASSERT_THAT(result, ElementsAre(200));

  result = lock_manager.ReleaseLocks(txn2);
  ASSERT_TRUE(result.empty());
}

TEST_F(DDRLockManagerTest, KeyReplicaLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = MakeTxnHolder(configs[0], 100, {}, {"writeA", "writeB"});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"readA"}, {"writeA"}, {{"readA", {1, 0}}, {"writeA", {1, 0}}});

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn2), AcquireLocksResult::ACQUIRED);
}

TEST_F(DDRLockManagerTest, RemasterTxn) {
  auto configs = MakeTestConfigurations("locking", 2, 1);
  auto txn = MakeTxnHolder(configs[0], 100, {}, {"A"});
  txn.transaction()->mutable_remaster()->set_new_master(1);
  auto txn_lockonly1 = MakeTxnHolder(configs[0], 100, {}, {"A"});
  txn_lockonly1.transaction()->mutable_remaster()->set_new_master(1);
  auto txn_lockonly2 = MakeTxnHolder(configs[0], 100, {}, {"A"});
  txn_lockonly1.transaction()->mutable_remaster()->set_new_master(1);
  txn_lockonly1.transaction()->mutable_remaster()->set_is_new_master_lock_only(true);

  ASSERT_FALSE(lock_manager.AcceptTransaction(txn));
  ASSERT_EQ(lock_manager.AcquireLocks(txn_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn_lockonly2), AcquireLocksResult::ACQUIRED);
  lock_manager.ReleaseLocks(txn);

  ASSERT_EQ(lock_manager.AcquireLocks(txn_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn));
  ASSERT_EQ(lock_manager.AcquireLocks(txn_lockonly2), AcquireLocksResult::ACQUIRED);
  lock_manager.ReleaseLocks(txn);

  ASSERT_EQ(lock_manager.AcquireLocks(txn_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn_lockonly2), AcquireLocksResult::WAITING);
  ASSERT_TRUE(lock_manager.AcceptTransaction(txn));
  lock_manager.ReleaseLocks(txn);
}

TEST_F(DDRLockManagerTest, EnsureStateIsClean) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = MakeTxnHolder(configs[0], 100, {"A"}, {"B", "C"});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"B"}, {"A"});
  auto txn3 = MakeTxnHolder(configs[0], 300, {}, {"C"});

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn1), AcquireLocksResult::ACQUIRED);
  ASSERT_TRUE(lock_manager.ReleaseLocks(txn1).empty());

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn2), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn3), AcquireLocksResult::ACQUIRED);
  ASSERT_TRUE(lock_manager.ReleaseLocks(txn2).empty());
  ASSERT_TRUE(lock_manager.ReleaseLocks(txn3).empty());
}

TEST_F(DDRLockManagerTest, LongChain) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = MakeTxnHolder(configs[0], 100, {}, {"A"});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"A"}, {});
  auto txn3 = MakeTxnHolder(configs[0], 300, {"A"}, {});
  auto txn4 = MakeTxnHolder(configs[0], 400, {}, {"A"});
  auto txn5 = MakeTxnHolder(configs[0], 500, {"A"}, {});

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn2), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn3), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn4), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn5), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(txn1);
  ASSERT_THAT(result, UnorderedElementsAre(200, 300));

  ASSERT_TRUE(lock_manager.ReleaseLocks(txn2).empty());
  result = lock_manager.ReleaseLocks(txn3);
  ASSERT_THAT(result, ElementsAre(400));

  result = lock_manager.ReleaseLocks(txn4);
  ASSERT_THAT(result, ElementsAre(500));

  ASSERT_TRUE(lock_manager.ReleaseLocks(txn5).empty());
}