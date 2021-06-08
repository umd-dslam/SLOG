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
  auto holder = MakeTestTxnHolder(
      configs[0], 100, {{"readA", KeyType::READ, 0}, {"readB", KeyType::READ, 0}, {"writeC", KeyType::WRITE, 0}});
  ASSERT_EQ(lock_manager.AcquireLocks(holder.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  auto result = lock_manager.ReleaseLocks(holder.txn_id());
  ASSERT_TRUE(result.empty());
}

TEST_F(DDRLockManagerTest, ReadLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"readA", KeyType::READ, 0}, {"readB", KeyType::READ, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"readB", KeyType::READ, 0}, {"readC", KeyType::READ, 0}});
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder1.txn_id()).empty());
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder2.txn_id()).empty());
}

TEST_F(DDRLockManagerTest, WriteLocks) {
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

TEST_F(DDRLockManagerTest, ReleaseLocksAndReturnMultipleNewLockHolders) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto holder1 =
      MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::READ, 0}, {"B", KeyType::WRITE, 0}, {"C", KeyType::WRITE, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"B", KeyType::READ, 0}, {"A", KeyType::WRITE, 0}});
  auto holder3 = MakeTestTxnHolder(configs[0], 300, {{"A", KeyType::WRITE, 0}});
  auto holder4 = MakeTestTxnHolder(configs[0], 400, {{"C", KeyType::READ, 0}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder3.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder4.lock_only_txn(0)), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(holder1.txn_id());
  ASSERT_EQ(result.size(), 2U);
  ASSERT_TRUE(find(result.begin(), result.end(), 200) != result.end());
  ASSERT_TRUE(find(result.begin(), result.end(), 400) != result.end());

  ASSERT_TRUE(lock_manager.ReleaseLocks(holder4.txn_id()).empty());

  result = lock_manager.ReleaseLocks(holder2.txn_id());
  ASSERT_THAT(result, ElementsAre(300));

  ASSERT_TRUE(lock_manager.ReleaseLocks(holder3.txn_id()).empty());
}

TEST_F(DDRLockManagerTest, PartiallyAcquiredLocks) {
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

TEST_F(DDRLockManagerTest, AcquireLocksWithLockOnly1) {
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

TEST_F(DDRLockManagerTest, AcquireLocksWithLockOnly2) {
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

TEST_F(DDRLockManagerTest, MultiEdgeBetweenTwoTxns) {
  auto configs = MakeTestConfigurations("locking", 3, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::WRITE, 1}, {"B", KeyType::WRITE, 2}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A", KeyType::READ, 1}, {"B", KeyType::READ, 2}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(2)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(2)), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(holder1.txn_id());
  ASSERT_THAT(result, ElementsAre(200));

  result = lock_manager.ReleaseLocks(holder2.txn_id());
  ASSERT_TRUE(result.empty());
}

TEST_F(DDRLockManagerTest, KeyReplicaLocks) {
  auto configs = MakeTestConfigurations("locking", 3, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"writeA", KeyType::WRITE, 2}, {"writeB", KeyType::WRITE, 2}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"readA", KeyType::READ, 1}, {"writeA", KeyType::WRITE, 1}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(2)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::ACQUIRED);
}

#ifdef REMASTER_PROTOCOL_COUNTERLESS
TEST_F(DDRLockManagerTest, RemasterTxn) {
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

TEST_F(DDRLockManagerTest, EnsureStateIsClean) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto holder1 =
      MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::READ, 0}, {"B", KeyType::WRITE, 0}, {"C", KeyType::WRITE, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"B", KeyType::READ, 0}, {"A", KeyType::WRITE, 0}});
  auto holder3 = MakeTestTxnHolder(configs[0], 300, {{"C", KeyType::WRITE, 0}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder1.txn_id()).empty());

  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder3.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder2.txn_id()).empty());
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder3.txn_id()).empty());
}

TEST_F(DDRLockManagerTest, LongChain) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::WRITE, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A", KeyType::READ, 0}});
  auto holder3 = MakeTestTxnHolder(configs[0], 300, {{"A", KeyType::READ, 0}});
  auto holder4 = MakeTestTxnHolder(configs[0], 400, {{"A", KeyType::WRITE, 0}});
  auto holder5 = MakeTestTxnHolder(configs[0], 500, {{"A", KeyType::READ, 0}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder3.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder4.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder5.lock_only_txn(0)), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(holder1.txn_id());
  ASSERT_THAT(result, UnorderedElementsAre(200, 300));

  ASSERT_TRUE(lock_manager.ReleaseLocks(holder2.txn_id()).empty());
  result = lock_manager.ReleaseLocks(holder3.txn_id());
  ASSERT_THAT(result, ElementsAre(400));

  result = lock_manager.ReleaseLocks(holder4.txn_id());
  ASSERT_THAT(result, ElementsAre(500));

  ASSERT_TRUE(lock_manager.ReleaseLocks(holder5.txn_id()).empty());
}