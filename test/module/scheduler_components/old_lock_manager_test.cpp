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
  auto holder = MakeTestTxnHolder(
      configs[0], 1000, {{"readA", KeyType::READ, 0}, {"readB", KeyType::READ, 0}, {"writeC", KeyType::WRITE, 0}});
  ASSERT_EQ(lock_manager.AcquireLocks(holder.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  auto result = lock_manager.ReleaseLocks(holder.txn_id());
  ASSERT_TRUE(result.empty());
}

TEST(OldLockManager, ReadLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"readA", KeyType::READ, 0}, {"readB", KeyType::READ, 0}});

  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"readB", KeyType::READ, 0}, {"readC", KeyType::READ, 0}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder1.txn_id()).empty());
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder2.txn_id()).empty());
}

TEST(OldLockManager, WriteLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"writeA", KeyType::WRITE, 0}, {"writeB", KeyType::WRITE, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"readA", KeyType::READ, 0}, {"writeA", KeyType::WRITE, 0}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  // The blocked txn becomes ready
  ASSERT_EQ(lock_manager.ReleaseLocks(holder1.txn_id()).size(), 1U);
  // Make sure the lock is already held by holder2
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::WAITING);
}

TEST(OldLockManager, ReleaseLocksAndGetManyNewHolders) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;
  auto holder1 =
      MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::READ, 0}, {"B", KeyType::WRITE, 0}, {"C", KeyType::WRITE, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"B", KeyType::READ, 0}, {"A", KeyType::WRITE, 0}});
  auto holder3 = MakeTestTxnHolder(configs[0], 300, {{"B", KeyType::READ, 0}});
  auto holder4 = MakeTestTxnHolder(configs[0], 400, {{"C", KeyType::READ, 0}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder3.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder4.lock_only_txn(0)), AcquireLocksResult::WAITING);

  ASSERT_TRUE(lock_manager.ReleaseLocks(holder3.txn_id()).empty());

  auto ready_txns = lock_manager.ReleaseLocks(holder1.txn_id());
  // Txn 300 was removed from the wait list due to the
  // ReleaseLocks call above
  ASSERT_EQ(ready_txns.size(), 2U);
  ASSERT_THAT(ready_txns, UnorderedElementsAre(200, 400));
}

TEST(OldLockManager, PartiallyAcquiredLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;
  auto holder1 =
      MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::READ, 0}, {"B", KeyType::WRITE, 0}, {"C", KeyType::WRITE, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A", KeyType::READ, 0}, {"B", KeyType::WRITE, 0}});
  auto holder3 = MakeTestTxnHolder(configs[0], 300, {{"A", KeyType::WRITE, 0}, {"C", KeyType::WRITE, 0}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder3.lock_only_txn(0)), AcquireLocksResult::WAITING);

  auto ready_txns = lock_manager.ReleaseLocks(holder1.txn_id());
  ASSERT_THAT(ready_txns, ElementsAre(200));

  ready_txns = lock_manager.ReleaseLocks(holder2.txn_id());
  ASSERT_THAT(ready_txns, ElementsAre(300));
}

TEST(OldLockManager, AcquireLocksWithLockOnlyholder1) {
  auto configs = MakeTestConfigurations("locking", 3, 1);
  OldLockManager lock_manager;
  auto holder1 =
      MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::READ, 0}, {"B", KeyType::WRITE, 0}, {"C", KeyType::WRITE, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A", KeyType::READ, 2}, {"B", KeyType::WRITE, 1}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(2)), AcquireLocksResult::ACQUIRED);

  auto ready_txns = lock_manager.ReleaseLocks(holder2.txn_id());
  ASSERT_THAT(ready_txns, ElementsAre(100));
}

TEST(OldLockManager, AcquireLocksWithLockOnlyholder2) {
  auto configs = MakeTestConfigurations("locking", 3, 1);
  OldLockManager lock_manager;
  auto holder1 =
      MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::READ, 0}, {"B", KeyType::WRITE, 0}, {"C", KeyType::WRITE, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A", KeyType::READ, 2}, {"B", KeyType::WRITE, 1}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(2)), AcquireLocksResult::WAITING);

  auto ready_txns = lock_manager.ReleaseLocks(holder1.txn_id());
  ASSERT_THAT(ready_txns, ElementsAre(200));
}
