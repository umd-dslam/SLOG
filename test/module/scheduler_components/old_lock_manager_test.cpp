#include "module/scheduler_components/old_lock_manager.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common/proto_utils.h"
#include "test/test_utils.h"

using namespace std;
using namespace slog;
using testing::ElementsAre;
using testing::UnorderedElementsAre;

TEST(OldLockManagerTest, GetAllLocksOnFirstTry) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;
  auto holder = MakeTestTxnHolder(configs[0], 100, {{"readA"}, {"readB"}}, {{"writeC"}});
  ASSERT_EQ(lock_manager.AcquireLocks(holder.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  auto result = lock_manager.ReleaseLocks(holder);
  ASSERT_TRUE(result.empty());
}

TEST(OldLockManagerTest, GetAllLocksMultiPartitions) {
  auto configs = MakeTestConfigurations("locking", 1, 2);
  OldLockManager lock_manager;
  // "AAAA" is in partition 0 so lock is acquired
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"readX"}}, {{"AAAA"}});
  // "ZZZZ" is in partition 1 so is ignored
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"readX"}}, {{"ZZZZ"}});
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
}

TEST(OldLockManager, ReadLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"readA"}, {"readB"}}, {});

  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"readB"}, {"readC"}}, {});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder1).empty());
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder2).empty());
}

TEST(OldLockManager, WriteLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {}, {{"writeA"}, {"writeB"}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"readA"}}, {{"writeA"}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  // The blocked txn becomes ready
  ASSERT_EQ(lock_manager.ReleaseLocks(holder1).size(), 1U);
  // Make sure the lock is already held by holder2
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::WAITING);
}

TEST(OldLockManager, ReleaseLocksAndGetManyNewHolders) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"A"}}, {{"B"}, {"C"}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"B"}}, {{"A"}});
  auto holder3 = MakeTestTxnHolder(configs[0], 300, {{"B"}}, {});
  auto holder4 = MakeTestTxnHolder(configs[0], 400, {{"C"}}, {});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder3.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder4.lock_only_txn(0)), AcquireLocksResult::WAITING);

  ASSERT_TRUE(lock_manager.ReleaseLocks(holder3).empty());

  auto ready_txns = lock_manager.ReleaseLocks(holder1);
  // Txn 300 was removed from the wait list due to the
  // ReleaseLocks call above
  ASSERT_EQ(ready_txns.size(), 2U);
  ASSERT_THAT(ready_txns, UnorderedElementsAre(200, 400));
}

TEST(OldLockManager, PartiallyAcquiredLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"A"}}, {{"B"}, {"C"}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A"}}, {{"B"}});
  auto holder3 = MakeTestTxnHolder(configs[0], 300, {}, {{"A"}, {"C"}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder3.lock_only_txn(0)), AcquireLocksResult::WAITING);

  auto ready_txns = lock_manager.ReleaseLocks(holder1);
  ASSERT_THAT(ready_txns, ElementsAre(200));

  ready_txns = lock_manager.ReleaseLocks(holder2);
  ASSERT_THAT(ready_txns, ElementsAre(300));
}

TEST(OldLockManager, PrioritizeWriteLock) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"A"}}, {{"A"}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A"}}, {});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);

  auto ready_txns = lock_manager.ReleaseLocks(holder1);
  ASSERT_THAT(ready_txns, ElementsAre(200));
}

TEST(OldLockManager, AcquireLocksWithLockOnlyholder1) {
  auto configs = MakeTestConfigurations("locking", 3, 1);
  OldLockManager lock_manager;
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"A"}}, {{"B"}, {"C"}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A", 2}}, {{"B", 1}});
  auto holder2_lockonly1 = MakeTestTxnHolder(configs[0], 200, {}, {{"B"}});
  auto holder2_lockonly2 = MakeTestTxnHolder(configs[0], 200, {{"A"}}, {});

  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(2)), AcquireLocksResult::ACQUIRED);

  auto ready_txns = lock_manager.ReleaseLocks(holder2);
  ASSERT_THAT(ready_txns, ElementsAre(100));
}

TEST(OldLockManager, AcquireLocksWithLockOnlyholder2) {
  auto configs = MakeTestConfigurations("locking", 3, 1);
  OldLockManager lock_manager;
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"A"}}, {{"B"}, {"C"}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A", 2}}, {{"B", 1}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(2)), AcquireLocksResult::WAITING);

  auto ready_txns = lock_manager.ReleaseLocks(holder1);
  ASSERT_THAT(ready_txns, ElementsAre(200));
}

TEST(OldLockManager, GhostTxns) {
  auto configs = MakeTestConfigurations("locking", 1, 2);
  OldLockManager lock_manager;
  // "Z" is in partition 1
  auto holder = MakeTestTxnHolder(configs[0], 101, {{"Z"}}, {});
  ASSERT_EQ(lock_manager.AcquireLocks(holder.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
}
