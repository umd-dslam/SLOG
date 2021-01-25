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
  auto holder = MakeTestTxnHolder(configs[0], 100, {{"readA"}, {"readB"}}, {{"writeC"}});
  ASSERT_EQ(lock_manager.AcquireLocks(holder.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  auto result = lock_manager.ReleaseLocks(holder);
  ASSERT_TRUE(result.empty());
}

TEST_F(DDRLockManagerTest, ReadLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"readA"}, {"readB"}}, {});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"readB"}, {"readC"}}, {});
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder1).empty());
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder2).empty());
}

TEST_F(DDRLockManagerTest, WriteLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {}, {{"writeA"}, {"writeB"}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"readA"}}, {{"writeA"}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  // The blocked txn becomes ready
  ASSERT_EQ(lock_manager.ReleaseLocks(holder1).size(), 1U);
  // Make sure the lock is already held by holder2
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::WAITING);
}

TEST_F(DDRLockManagerTest, ReleaseLocksAndReturnMultipleNewLockHolders) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"A"}}, {{"B"}, {"C"}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"B"}}, {{"A"}});
  auto holder3 = MakeTestTxnHolder(configs[0], 300, {}, {{"A"}});
  auto holder4 = MakeTestTxnHolder(configs[0], 400, {{"C"}}, {});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder3.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder4.lock_only_txn(0)), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(holder1);
  ASSERT_EQ(result.size(), 2U);
  ASSERT_TRUE(find(result.begin(), result.end(), 200) != result.end());
  ASSERT_TRUE(find(result.begin(), result.end(), 400) != result.end());

  ASSERT_TRUE(lock_manager.ReleaseLocks(holder4).empty());

  result = lock_manager.ReleaseLocks(holder2);
  ASSERT_THAT(result, ElementsAre(300));

  ASSERT_TRUE(lock_manager.ReleaseLocks(holder3).empty());
}

TEST_F(DDRLockManagerTest, PartiallyAcquiredLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"A"}}, {{"B"}, {"C"}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A"}}, {{"B"}});
  auto holder3 = MakeTestTxnHolder(configs[0], 300, {}, {{"A"}, {"C"}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder3.lock_only_txn(0)), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(holder1);
  ASSERT_THAT(result, ElementsAre(200));

  result = lock_manager.ReleaseLocks(holder2);
  ASSERT_THAT(result, ElementsAre(300));
}

TEST_F(DDRLockManagerTest, PrioritizeWriteLock) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"A"}}, {{"A"}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A"}}, {});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(holder1);
  ASSERT_THAT(result, ElementsAre(200));
}

TEST_F(DDRLockManagerTest, AcquireLocksWithLockOnlyholder1) {
  auto configs = MakeTestConfigurations("locking", 2, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"A"}}, {{"B"}, {"C"}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A", 1}}, {{"B", 0}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::ACQUIRED);

  auto result = lock_manager.ReleaseLocks(holder2);
  ASSERT_THAT(result, ElementsAre(100));
}

TEST_F(DDRLockManagerTest, AcquireLocksWithLockOnlyholder2) {
  auto configs = MakeTestConfigurations("locking", 2, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"A"}}, {{"B"}, {"C"}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A", 1}}, {{"B", 0}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(holder1);
  ASSERT_THAT(result, ElementsAre(200));
}

TEST_F(DDRLockManagerTest, MultiEdgeBetweenTwoTxns) {
  auto configs = MakeTestConfigurations("locking", 3, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {}, {{"A", 1}, {"B", 2}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A", 1}, {"B", 2}}, {});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(2)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(2)), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(holder1);
  ASSERT_THAT(result, ElementsAre(200));

  result = lock_manager.ReleaseLocks(holder2);
  ASSERT_TRUE(result.empty());
}

TEST_F(DDRLockManagerTest, KeyReplicaLocks) {
  auto configs = MakeTestConfigurations("locking", 3, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {}, {{"writeA", 2}, {"writeB", 2}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"readA", 1}}, {{"writeA", 1}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(2)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::ACQUIRED);
}

#ifdef REMASTER_PROTOCOL_COUNTERLESS
TEST_F(DDRLockManagerTest, RemasterTxn) {
  auto configs = MakeTestConfigurations("locking", 3, 1);
  auto txn = MakeTestTxnHolder(configs[0], 100, {}, {{"A", 2}}, 1 /* new_master */);

  ASSERT_EQ(lock_manager.AcquireLocks(txn.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn.lock_only_txn(2)), AcquireLocksResult::ACQUIRED);
  lock_manager.ReleaseLocks(txn);

  ASSERT_EQ(lock_manager.AcquireLocks(txn.lock_only_txn(2)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn.lock_only_txn(1)), AcquireLocksResult::ACQUIRED);
  lock_manager.ReleaseLocks(txn);
}
#endif

TEST_F(DDRLockManagerTest, EnsureStateIsClean) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"A"}}, {{"B"}, {"C"}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"B"}}, {{"A"}});
  auto holder3 = MakeTestTxnHolder(configs[0], 300, {}, {{"C"}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder1).empty());

  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder3.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder2).empty());
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder3).empty());
}

TEST_F(DDRLockManagerTest, LongChain) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {}, {{"A"}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A"}}, {});
  auto holder3 = MakeTestTxnHolder(configs[0], 300, {{"A"}}, {});
  auto holder4 = MakeTestTxnHolder(configs[0], 400, {}, {{"A"}});
  auto holder5 = MakeTestTxnHolder(configs[0], 500, {{"A"}}, {});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder3.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder4.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder5.lock_only_txn(0)), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(holder1);
  ASSERT_THAT(result, UnorderedElementsAre(200, 300));

  ASSERT_TRUE(lock_manager.ReleaseLocks(holder2).empty());
  result = lock_manager.ReleaseLocks(holder3);
  ASSERT_THAT(result, ElementsAre(400));

  result = lock_manager.ReleaseLocks(holder4);
  ASSERT_THAT(result, ElementsAre(500));

  ASSERT_TRUE(lock_manager.ReleaseLocks(holder5).empty());
}