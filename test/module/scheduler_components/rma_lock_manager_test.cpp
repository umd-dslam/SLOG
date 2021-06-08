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
  auto holder = MakeTestTxnHolder(
      configs[0], 100, {{"readA", KeyType::READ, 0}, {"readB", KeyType::READ, 0}, {"writeC", KeyType::WRITE, 0}});
  ASSERT_EQ(lock_manager.AcquireLocks(holder.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  auto result = lock_manager.ReleaseLocks(holder.txn_id());
  ASSERT_TRUE(result.empty());
}

TEST(RMALockManagerTest, ReadLocks) {
  RMALockManager lock_manager;
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"readA", KeyType::READ, 0}, {"readB", KeyType::READ, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"readB", KeyType::READ, 0}, {"readC", KeyType::READ, 0}});
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder1.txn_id()).empty());
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder2.txn_id()).empty());
}

TEST(RMALockManagerTest, WriteLocks) {
  RMALockManager lock_manager;
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"writeA", KeyType::WRITE, 0}, {"writeB", KeyType::WRITE, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"readA", KeyType::READ, 0}, {"writeA", KeyType::WRITE, 0}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  // The blocked txn becomes ready
  ASSERT_EQ(lock_manager.ReleaseLocks(holder1.txn_id()).size(), 1U);
  // Make sure the lock is already held by holder2
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::WAITING);
}

TEST(RMALockManagerTest, ReleaseLocksAndGetMultipleNewLockHolders) {
  RMALockManager lock_manager;
  auto configs = MakeTestConfigurations("locking", 1, 1);
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

  auto result = lock_manager.ReleaseLocks(holder1.txn_id());
  // Txn 300 was removed from the wait list due to the
  // ReleaseLocks call above
  ASSERT_THAT(result, UnorderedElementsAre(200, 400));
}

TEST(RMALockManagerTest, PartiallyAcquiredLocks) {
  RMALockManager lock_manager;
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto holder1 =
      MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::READ, 0}, {"B", KeyType::WRITE, 0}, {"C", KeyType::WRITE, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A", KeyType::READ, 0}, {"B", KeyType::WRITE, 0}});
  auto holder3 = MakeTestTxnHolder(configs[0], 300, {{"A", KeyType::WRITE, 0}, {"C", KeyType::WRITE, 0}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder3.lock_only_txn(0)), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(holder1.txn_id());
  ASSERT_THAT(result, ElementsAre(200));

  result = lock_manager.ReleaseLocks(holder2.txn_id());
  ASSERT_THAT(result, ElementsAre(300));
}

TEST(RMALockManagerTest, AcquireLocksWithLockOnly1) {
  RMALockManager lock_manager;
  auto configs = MakeTestConfigurations("locking", 2, 1);
  auto holder1 =
      MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::READ, 0}, {"B", KeyType::WRITE, 0}, {"C", KeyType::WRITE, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A", KeyType::READ, 1}, {"B", KeyType::WRITE, 0}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::ACQUIRED);

  auto result = lock_manager.ReleaseLocks(holder2.txn_id());
  ASSERT_THAT(result, ElementsAre(100));
}

TEST(RMALockManagerTest, AcquireLocksWithLockOnly2) {
  RMALockManager lock_manager;
  auto configs = MakeTestConfigurations("locking", 2, 1);
  auto holder1 =
      MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::READ, 0}, {"B", KeyType::WRITE, 0}, {"C", KeyType::WRITE, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A", KeyType::READ, 1}, {"B", KeyType::WRITE, 0}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(holder1.txn_id());
  ASSERT_THAT(result, ElementsAre(200));
}

TEST(RMALockManagerTest, KeyReplicaLocks) {
  RMALockManager lock_manager;
  auto configs = MakeTestConfigurations("locking", 3, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"writeA", KeyType::WRITE, 2}, {"writeB", KeyType::WRITE, 2}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"readA", KeyType::READ, 1}, {"writeA", KeyType::WRITE, 1}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(2)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::ACQUIRED);
}

#ifdef REMASTER_PROTOCOL_COUNTERLESS
TEST(RMALockManagerTest, RemasterTxn) {
  RMALockManager lock_manager;
  auto configs = MakeTestConfigurations("locking", 3, 1);
  auto holder = MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::WRITE, 2}}, {}, 1 /* new_master */);

  ASSERT_EQ(lock_manager.AcquireLocks(holder.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder.lock_only_txn(2)), AcquireLocksResult::ACQUIRED);
  lock_manager.ReleaseLocks(holder.txn_id());

  ASSERT_EQ(lock_manager.AcquireLocks(holder.lock_only_txn(2)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder.lock_only_txn(1)), AcquireLocksResult::ACQUIRED);
  lock_manager.ReleaseLocks(holder.txn_id());
}
#endif