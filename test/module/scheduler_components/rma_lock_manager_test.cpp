#include "module/scheduler_components/rma_lock_manager.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common/proto_utils.h"
#include "test/test_utils.h"

using namespace std;
using namespace slog;
using testing::ElementsAre;
using testing::UnorderedElementsAre;

TEST(RMALockManagerTest, GetAllLocksOnFirstTry) {
  RMALockManager lock_manager;
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto holder = MakeTestTxnHolder(configs[0], 100, {{"readA"}, {"readB"}}, {{"writeC"}});
  ASSERT_EQ(lock_manager.AcquireLocks(holder.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  auto result = lock_manager.ReleaseLocks(holder);
  ASSERT_TRUE(result.empty());
}

TEST(RMALockManagerTest, ReadLocks) {
  RMALockManager lock_manager;
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"readA"}, {"readB"}}, {});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"readB"}, {"readC"}}, {});
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder1).empty());
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder2).empty());
}

TEST(RMALockManagerTest, WriteLocks) {
  RMALockManager lock_manager;
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

TEST(RMALockManagerTest, ReleaseLocksAndGetMultipleNewLockHolders) {
  RMALockManager lock_manager;
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"A"}}, {{"B"}, {"C"}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"B"}}, {{"A"}});
  auto holder3 = MakeTestTxnHolder(configs[0], 300, {{"B"}}, {});
  auto holder4 = MakeTestTxnHolder(configs[0], 400, {{"C"}}, {});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder3.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder4.lock_only_txn(0)), AcquireLocksResult::WAITING);

  ASSERT_TRUE(lock_manager.ReleaseLocks(holder3).empty());

  auto result = lock_manager.ReleaseLocks(holder1);
  // Txn 300 was removed from the wait list due to the
  // ReleaseLocks call above
  ASSERT_THAT(result, UnorderedElementsAre(200, 400));
}

TEST(RMALockManagerTest, PartiallyAcquiredLocks) {
  RMALockManager lock_manager;
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

TEST(RMALockManagerTest, PrioritizeWriteLock) {
  RMALockManager lock_manager;
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"A"}}, {{"A"}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A"}}, {});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(holder1);
  ASSERT_THAT(result, ElementsAre(200));
}

TEST(RMALockManagerTest, AcquireLocksWithLockOnlyholder1) {
  RMALockManager lock_manager;
  auto configs = MakeTestConfigurations("locking", 2, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"A"}}, {{"B"}, {"C"}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A", 1}}, {{"B", 0}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::ACQUIRED);

  auto result = lock_manager.ReleaseLocks(holder2);
  ASSERT_THAT(result, ElementsAre(100));
}

TEST(RMALockManagerTest, AcquireLocksWithLockOnlyholder2) {
  RMALockManager lock_manager;
  auto configs = MakeTestConfigurations("locking", 2, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"A"}}, {{"B"}, {"C"}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A", 1}}, {{"B", 0}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(holder1);
  ASSERT_THAT(result, ElementsAre(200));
}

TEST(RMALockManagerTest, KeyReplicaLocks) {
  RMALockManager lock_manager;
  auto configs = MakeTestConfigurations("locking", 3, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {}, {{"writeA", 2}, {"writeB", 2}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"readA", 1}}, {{"writeA", 1}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(2)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::ACQUIRED);
}

#ifdef REMASTER_PROTOCOL_COUNTERLESS
TEST(RMALockManagerTest, RemasterTxn) {
  RMALockManager lock_manager;
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