#include "module/scheduler_components/old_lock_manager.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common/proto_utils.h"
#include "test/test_utils.h"

using namespace std;
using namespace slog;
using testing::ElementsAre;
using testing::UnorderedElementsAre;

MessagePool<internal::Request> pool;

TransactionHolder MakeTxnHolder(const ConfigurationPtr& config, TxnId id, const unordered_set<Key>& read_set,
                                  const unordered_set<Key>& write_set) {
    auto txn = MakeTransaction(read_set, write_set);
    txn->mutable_internal()->set_id(id);
    ReusableRequest req(&pool);
    req.get()->mutable_forward_txn()->set_allocated_txn(txn);
    return {config, move(req)};
  }

TEST(OldLockManagerTest, GetAllLocksOnFirstTry) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;
  auto txn = MakeTxnHolder(configs[0], 100, {"readA", "readB"}, {"writeC"});
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn), AcquireLocksResult::ACQUIRED);
  auto result = lock_manager.ReleaseLocks(txn);
  ASSERT_TRUE(result.empty());
}

TEST(OldLockManagerTest, GetAllLocksMultiPartitions) {
  auto configs = MakeTestConfigurations("locking", 1, 2);
  OldLockManager lock_manager;
  // "AAAA" is in partition 0 so lock is acquired
  auto txn1 = MakeTxnHolder(configs[0], 100, {"readX"}, {"AAAA"});
  // "ZZZZ" is in partition 1 so is ignored
  auto txn2 = MakeTxnHolder(configs[0], 200, {"readX"}, {"ZZZZ"});
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn2), AcquireLocksResult::WAITING);
}

TEST(OldLockManager, ReadLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;
  auto txn1 = MakeTxnHolder(configs[0], 100, {"readA", "readB"}, {});

  auto txn2 = MakeTxnHolder(configs[0], 200, {"readB", "readC"}, {});

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn2), AcquireLocksResult::ACQUIRED);
  ASSERT_TRUE(lock_manager.ReleaseLocks(txn1).empty());
  ASSERT_TRUE(lock_manager.ReleaseLocks(txn2).empty());
}

TEST(OldLockManager, WriteLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;
  auto txn1 = MakeTxnHolder(configs[0], 100, {}, {"writeA", "writeB"});

  auto txn2 = MakeTxnHolder(configs[0], 200, {"readA"}, {"writeA"});

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn2), AcquireLocksResult::WAITING);
  // The blocked txn becomes ready
  ASSERT_EQ(lock_manager.ReleaseLocks(txn1).size(), 1U);
  // Make sure the lock is already held by txn2
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn1), AcquireLocksResult::WAITING);
}

TEST(OldLockManager, ReleaseLocksAndGetManyNewHolders) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;
  auto txn1 = MakeTxnHolder(configs[0], 100, {"A"}, {"B", "C"});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"B"}, {"A"});
  auto txn3 = MakeTxnHolder(configs[0], 300, {"B"}, {});
  auto txn4 = MakeTxnHolder(configs[0], 400, {"C"}, {});

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn2), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn3), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn4), AcquireLocksResult::WAITING);

  ASSERT_TRUE(lock_manager.ReleaseLocks(txn3).empty());

  auto ready_txns = lock_manager.ReleaseLocks(txn1);
  // Txn 300 was removed from the wait list due to the
  // ReleaseLocks call above
  ASSERT_EQ(ready_txns.size(), 2U);
  ASSERT_THAT(ready_txns, UnorderedElementsAre(200, 400));
}

TEST(OldLockManager, PartiallyAcquiredLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;
  auto txn1 = MakeTxnHolder(configs[0], 100, {"A"}, {"B", "C"});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"A"}, {"B"});
  auto txn3 = MakeTxnHolder(configs[0], 300, {}, {"A", "C"});

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn2), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn3), AcquireLocksResult::WAITING);

  auto ready_txns = lock_manager.ReleaseLocks(txn1);
  ASSERT_THAT(ready_txns, ElementsAre(200));

  ready_txns = lock_manager.ReleaseLocks(txn2);
  ASSERT_THAT(ready_txns, ElementsAre(300));
}

TEST(OldLockManager, PrioritizeWriteLock) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;
  auto txn1 = MakeTxnHolder(configs[0], 100, {"A"}, {"A"});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"A"}, {});

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn2), AcquireLocksResult::WAITING);

  auto ready_txns = lock_manager.ReleaseLocks(txn1);
  ASSERT_THAT(ready_txns, ElementsAre(200));
}

TEST(OldLockManager, AcquireLocksWithLockOnlyTxn1) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;
  auto txn1 = MakeTxnHolder(configs[0], 100, {"A"}, {"B", "C"});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"A"}, {"B"});
  auto txn2_lockonly1 = MakeTxnHolder(configs[0], 200, {}, {"B"});
  auto txn2_lockonly2 = MakeTxnHolder(configs[0], 200, {"A"}, {});

  ASSERT_FALSE(lock_manager.AcceptTransaction(txn1));
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn2));
  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly2), AcquireLocksResult::ACQUIRED);

  auto ready_txns = lock_manager.ReleaseLocks(txn2);
  ASSERT_THAT(ready_txns, ElementsAre(100));
}

TEST(OldLockManager, AcquireLocksWithLockOnlyTxn2) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;
  auto txn1 = MakeTxnHolder(configs[0], 100, {"A"}, {"B", "C"});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"A"}, {"B"});
  auto txn2_lockonly1 = MakeTxnHolder(configs[0], 200, {}, {"B"});
  auto txn2_lockonly2 = MakeTxnHolder(configs[0], 200, {"A"}, {});

  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly2), AcquireLocksResult::WAITING);
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn1));
  ASSERT_TRUE(lock_manager.AcceptTransaction(txn2));

  auto ready_txns = lock_manager.ReleaseLocks(txn2);
  ASSERT_THAT(ready_txns, ElementsAre(100));
}

TEST(OldLockManager, AcquireLocksWithLockOnlyTxnOutOfOrder) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;
  auto txn1 = MakeTxnHolder(configs[0], 100, {"A"}, {"B", "C"});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"A"}, {"B"});
  auto txn2_lockonly1 = MakeTxnHolder(configs[0], 200, {}, {"B"});
  auto txn2_lockonly2 = MakeTxnHolder(configs[0], 200, {"A"}, {});

  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn2));
  ASSERT_EQ(lock_manager.AcquireLocks(txn1), AcquireLocksResult::WAITING);
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn1));
  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly2), AcquireLocksResult::ACQUIRED);

  auto ready_txns = lock_manager.ReleaseLocks(txn2);
  ASSERT_THAT(ready_txns, ElementsAre(100));
}

TEST(OldLockManager, GhostTxns) {
  auto configs = MakeTestConfigurations("locking", 1, 2);
  OldLockManager lock_manager;
  // "X" is in partition 1
  auto txn1 = MakeTxnHolder(configs[0], 100, {}, {"X"});
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn1));

  // "Z" is in partition 1
  auto txn2 = MakeTxnHolder(configs[0], 101, {"Z"}, {});
  ASSERT_EQ(lock_manager.AcquireLocks(txn2), AcquireLocksResult::WAITING);
}

TEST(OldLockManager, BlockedLockOnlyTxn) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;

  auto txn1 = MakeTxnHolder(configs[0], 100, {"A"}, {"B"});
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn1), AcquireLocksResult::ACQUIRED);

  auto txn2 = MakeTxnHolder(configs[0], 101, {}, {"B"});
  ASSERT_EQ(lock_manager.AcquireLocks(txn2), AcquireLocksResult::WAITING);
}