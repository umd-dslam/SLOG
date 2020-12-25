#include <gtest/gtest.h>

#include "test/test_utils.h"
#include "common/proto_utils.h"
#include "module/scheduler_components/old_lock_manager.h"

using namespace std;
using namespace slog;

MessagePool<internal::Request> pool;

TransactionHolder MakeHolder(const ConfigurationPtr& config, Transaction* txn) {
  ReusableRequest req(&pool);
  req.get()->mutable_forward_txn()->set_allocated_txn(txn);
  return TransactionHolder{config, move(req)};
}

TEST(OldLockManagerTest, GetAllLocksOnFirstTry) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;
  auto txn = MakeTransaction(
    {"readA", "readB"},
    {"writeC"});
  TransactionHolder holder = MakeHolder(configs[0], txn);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder), AcquireLocksResult::ACQUIRED);
  auto new_holders = lock_manager.ReleaseLocks(holder);
  ASSERT_TRUE(new_holders.empty());
}

TEST(OldLockManagerTest, GetAllLocksMultiPartitions) {
  auto configs = MakeTestConfigurations("locking", 1, 2);
  OldLockManager lock_manager;
  // "AAAA" is in partition 0 so lock is acquired
  auto txn1 = MakeTransaction({"readX"}, {"AAAA"});
  txn1->mutable_internal()->set_id(100);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);
  // "ZZZZ" is in partition 1 so is ignored
  auto txn2 = MakeTransaction({"readX"}, {"ZZZZ"});
  txn2->mutable_internal()->set_id(200);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder2), AcquireLocksResult::WAITING);
}

TEST(OldLockManager, ReadLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;
  auto txn1 = MakeTransaction({"readA", "readB"}, {});
  txn1->mutable_internal()->set_id(100);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);

  auto txn2 = MakeTransaction({"readB", "readC"}, {});
  txn2->mutable_internal()->set_id(200);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder2), AcquireLocksResult::ACQUIRED);
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder1).empty());
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder2).empty());
}

TEST(OldLockManager, WriteLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;
  auto txn1 = MakeTransaction({}, {"writeA", "writeB"});
  txn1->mutable_internal()->set_id(100);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);

  auto txn2 = MakeTransaction({"readA"}, {"writeA"});
  txn2->mutable_internal()->set_id(200);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder2), AcquireLocksResult::WAITING);
  // The blocked txn becomes ready
  ASSERT_EQ(lock_manager.ReleaseLocks(holder1).size(), 1U);
  // Make sure the lock is already held by txn2
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder1), AcquireLocksResult::WAITING);
}

TEST(OldLockManager, ReleaseLocksAndGetManyNewHolders) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;
  auto txn1 = MakeTransaction({"A"}, {"B", "C"});
  txn1->mutable_internal()->set_id(100);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);
  auto txn2 = MakeTransaction({"B"}, {"A"});
  txn2->mutable_internal()->set_id(200);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);
  auto txn3 = MakeTransaction({"B"}, {});
  txn3->mutable_internal()->set_id(300);
  TransactionHolder holder3 = MakeHolder(configs[0], txn3);
  auto txn4 = MakeTransaction({"C"}, {});
  txn4->mutable_internal()->set_id(400);
  TransactionHolder holder4 = MakeHolder(configs[0], txn4);

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder2), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder3), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder4), AcquireLocksResult::WAITING);

  ASSERT_TRUE(lock_manager.ReleaseLocks(holder3).empty());

  auto ready_txns = lock_manager.ReleaseLocks(holder1);
  // Txn 300 was removed from the wait list due to the
  // ReleaseLocks call above
  ASSERT_EQ(ready_txns.size(), 2U);
  ASSERT_TRUE(find(ready_txns.begin(), ready_txns.end(), 200) != ready_txns.end());
  ASSERT_TRUE(find(ready_txns.begin(), ready_txns.end(), 400) != ready_txns.end());
}

TEST(OldLockManager, PartiallyAcquiredLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;
  auto txn1 = MakeTransaction({"A"}, {"B", "C"});
  txn1->mutable_internal()->set_id(100);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);
  auto txn2 = MakeTransaction({"A"}, {"B"});
  txn2->mutable_internal()->set_id(200);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);
  auto txn3 = MakeTransaction({}, {"A", "C"});
  txn3->mutable_internal()->set_id(300);
  TransactionHolder holder3 = MakeHolder(configs[0], txn3);

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder2), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder3), AcquireLocksResult::WAITING);

  auto ready_txns = lock_manager.ReleaseLocks(holder1);
  ASSERT_EQ(ready_txns.size(), 1U);
  ASSERT_TRUE(find(ready_txns.begin(), ready_txns.end(), 200) != ready_txns.end());

  ready_txns = lock_manager.ReleaseLocks(holder2);
  ASSERT_EQ(ready_txns.size(), 1U);
  ASSERT_TRUE(find(ready_txns.begin(), ready_txns.end(), 300) != ready_txns.end());
}

TEST(OldLockManager, PrioritizeWriteLock) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;
  auto txn1 = MakeTransaction({"A"}, {"A"});
  txn1->mutable_internal()->set_id(100);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);
  auto txn2 = MakeTransaction({"A"}, {});
  txn2->mutable_internal()->set_id(200);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);
 
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder2), AcquireLocksResult::WAITING);

  auto ready_txns = lock_manager.ReleaseLocks(holder1);
  ASSERT_EQ(ready_txns.size(), 1U);
  ASSERT_TRUE(find(ready_txns.begin(), ready_txns.end(), 200) != ready_txns.end());
}

TEST(OldLockManager, AcquireLocksWithLockOnlyTxn1) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;
  auto txn1 = MakeTransaction({"A"}, {"B", "C"});
  txn1->mutable_internal()->set_id(100);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);
  auto txn2 = MakeTransaction({"A"}, {"B"});
  txn2->mutable_internal()->set_id(200);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);
  auto txn2_lockonly1 = MakeTransaction({}, {"B"});
  txn2_lockonly1->mutable_internal()->set_id(200);
  TransactionHolder holder2_lockonly1 = MakeHolder(configs[0], txn2_lockonly1);
  auto txn2_lockonly2 = MakeTransaction({"A"}, {});
  txn2_lockonly2->mutable_internal()->set_id(200);
  TransactionHolder holder2_lockonly2 = MakeHolder(configs[0], txn2_lockonly2);

  ASSERT_FALSE(lock_manager.AcceptTransaction(holder1));
  ASSERT_FALSE(lock_manager.AcceptTransaction(holder2));
  ASSERT_EQ(lock_manager.AcquireLocks(holder2_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2_lockonly2), AcquireLocksResult::ACQUIRED);

  auto ready_txns = lock_manager.ReleaseLocks(holder2);
  ASSERT_EQ(ready_txns.size(), 1U);
  ASSERT_TRUE(find(ready_txns.begin(), ready_txns.end(), 100) != ready_txns.end());
}

TEST(OldLockManager, AcquireLocksWithLockOnlyTxn2) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;
  auto txn1 = MakeTransaction({"A"}, {"B", "C"});
  txn1->mutable_internal()->set_id(100);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);
  auto txn2 = MakeTransaction({"A"}, {"B"});
  txn2->mutable_internal()->set_id(200);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);
  auto txn2_lockonly1 = MakeTransaction({}, {"B"});
  txn2_lockonly1->mutable_internal()->set_id(200);
  TransactionHolder holder2_lockonly1 = MakeHolder(configs[0], txn2_lockonly1);
  auto txn2_lockonly2 = MakeTransaction({"A"}, {});
  txn2_lockonly2->mutable_internal()->set_id(200);
  TransactionHolder holder2_lockonly2 = MakeHolder(configs[0], txn2_lockonly2);

  ASSERT_EQ(lock_manager.AcquireLocks(holder2_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2_lockonly2), AcquireLocksResult::WAITING);
  ASSERT_FALSE(lock_manager.AcceptTransaction(holder1));
  ASSERT_TRUE(lock_manager.AcceptTransaction(holder2));

  auto ready_txns = lock_manager.ReleaseLocks(holder2);
  ASSERT_EQ(ready_txns.size(), 1U);
  ASSERT_TRUE(find(ready_txns.begin(), ready_txns.end(), 100) != ready_txns.end());
}

TEST(OldLockManager, AcquireLocksWithLockOnlyTxnOutOfOrder) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;
  auto txn1 = MakeTransaction({"A"}, {"B", "C"});
  txn1->mutable_internal()->set_id(100);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);
  auto txn2 = MakeTransaction({"A"}, {"B"});
  txn2->mutable_internal()->set_id(200);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);
  auto txn2_lockonly1 = MakeTransaction({}, {"B"});
  txn2_lockonly1->mutable_internal()->set_id(200);
  TransactionHolder holder2_lockonly1 = MakeHolder(configs[0], txn2_lockonly1);
  auto txn2_lockonly2 = MakeTransaction({"A"}, {});
  txn2_lockonly2->mutable_internal()->set_id(200);
  TransactionHolder holder2_lockonly2 = MakeHolder(configs[0], txn2_lockonly2);

  ASSERT_EQ(lock_manager.AcquireLocks(holder2_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_FALSE(lock_manager.AcceptTransaction(holder2));
  ASSERT_EQ(lock_manager.AcquireLocks(holder1), AcquireLocksResult::WAITING);
  ASSERT_FALSE(lock_manager.AcceptTransaction(holder1));
  ASSERT_EQ(lock_manager.AcquireLocks(holder2_lockonly2), AcquireLocksResult::ACQUIRED);

  auto ready_txns = lock_manager.ReleaseLocks(holder2);
  ASSERT_EQ(ready_txns.size(), 1U);
  ASSERT_TRUE(find(ready_txns.begin(), ready_txns.end(), 100) != ready_txns.end());
}

TEST(OldLockManager, GhostTxns) {
  auto configs = MakeTestConfigurations("locking", 1, 2);
  OldLockManager lock_manager;
  // "X" is in partition 1
  auto txn1 = MakeTransaction({}, {"X"});
  txn1->mutable_internal()->set_id(100);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);
  ASSERT_FALSE(lock_manager.AcceptTransaction(holder1));

  // "Z" is in partition 1
  auto txn2 = MakeTransaction({"Z"}, {});
  txn2->mutable_internal()->set_id(101);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2), AcquireLocksResult::WAITING);
}

TEST(OldLockManager, BlockedLockOnlyTxn) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  OldLockManager lock_manager;

  auto txn1 = MakeTransaction({"A"}, {"B"});
  txn1->mutable_internal()->set_id(100);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(holder1), AcquireLocksResult::ACQUIRED);

  auto txn2 = MakeTransaction({}, {"B"});
  txn2->mutable_internal()->set_id(101);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2), AcquireLocksResult::WAITING);
}