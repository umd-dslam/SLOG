#include <gtest/gtest.h>

#include "test/test_utils.h"
#include "common/proto_utils.h"
#include "module/scheduler_components/lock_manager_deprecated.h"

using namespace std;
using namespace slog;

MessagePool<internal::Request> pool;

TransactionHolder MakeHolder(const ConfigurationPtr& config, Transaction* txn) {
  ReusableRequest req(&pool);
  req.get()->mutable_forward_txn()->set_allocated_txn(txn);
  return TransactionHolder{config, move(req)};
}

TEST(LockManagerDeprecatedTest, GetAllLocksOnFirstTry) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  LockManagerDeprecated lock_manager;
  auto txn = MakeTransaction(
    {"readA", "readB"},
    {"writeC"});
  TransactionHolder holder = MakeHolder(configs[0], txn);
  ASSERT_TRUE(lock_manager.AcceptTransactionAndAcquireLocks(holder));
  auto new_holders = lock_manager.ReleaseLocks(holder);
  ASSERT_TRUE(new_holders.empty());
}

TEST(LockManagerDeprecatedTest, GetAllLocksMultiPartitions) {
  auto configs = MakeTestConfigurations("locking", 1, 2);
  LockManagerDeprecated lock_manager;
  // "AAAA" is in partition 0 so lock is acquired
  auto txn1 = MakeTransaction({"readX"}, {"AAAA"});
  txn1->mutable_internal()->set_id(100);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);
  // "ZZZZ" is in partition 1 so is ignored
  auto txn2 = MakeTransaction({"readX"}, {"ZZZZ"});
  txn2->mutable_internal()->set_id(200);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);
  ASSERT_TRUE(lock_manager.AcceptTransactionAndAcquireLocks(holder1));
  ASSERT_FALSE(lock_manager.AcceptTransactionAndAcquireLocks(holder2));
}

TEST(LockManagerDeprecated, ReadLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  LockManagerDeprecated lock_manager;
  auto txn1 = MakeTransaction({"readA", "readB"}, {});
  txn1->mutable_internal()->set_id(100);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);

  auto txn2 = MakeTransaction({"readB", "readC"}, {});
  txn2->mutable_internal()->set_id(200);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);

  ASSERT_TRUE(lock_manager.AcceptTransactionAndAcquireLocks(holder1));
  ASSERT_TRUE(lock_manager.AcceptTransactionAndAcquireLocks(holder2));
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder1).empty());
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder2).empty());
}

TEST(LockManagerDeprecated, WriteLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  LockManagerDeprecated lock_manager;
  auto txn1 = MakeTransaction({}, {"writeA", "writeB"});
  txn1->mutable_internal()->set_id(100);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);

  auto txn2 = MakeTransaction({"readA"}, {"writeA"});
  txn2->mutable_internal()->set_id(200);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);

  ASSERT_TRUE(lock_manager.AcceptTransactionAndAcquireLocks(holder1));
  ASSERT_FALSE(lock_manager.AcceptTransactionAndAcquireLocks(holder2));
  // The blocked txn becomes ready
  ASSERT_EQ(lock_manager.ReleaseLocks(holder1).size(), 1U);
  // Make sure the lock is already held by txn2
  ASSERT_FALSE(lock_manager.AcceptTransactionAndAcquireLocks(holder1));
}

TEST(LockManagerDeprecated, ReleaseLocksAndGetManyNewHolders) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  LockManagerDeprecated lock_manager;
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

  ASSERT_TRUE(lock_manager.AcceptTransactionAndAcquireLocks(holder1));
  ASSERT_FALSE(lock_manager.AcceptTransactionAndAcquireLocks(holder2));
  ASSERT_FALSE(lock_manager.AcceptTransactionAndAcquireLocks(holder3));
  ASSERT_FALSE(lock_manager.AcceptTransactionAndAcquireLocks(holder4));

  ASSERT_TRUE(lock_manager.ReleaseLocks(holder3).empty());

  auto new_ready_txns = lock_manager.ReleaseLocks(holder1);
  // Txn 300 was removed from the wait list due to the
  // ReleaseLocks call above
  ASSERT_EQ(new_ready_txns.size(), 2U);
  ASSERT_TRUE(new_ready_txns.count(200) > 0);
  ASSERT_TRUE(new_ready_txns.count(400) > 0);
}

TEST(LockManagerDeprecated, PartiallyAcquiredLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  LockManagerDeprecated lock_manager;
  auto txn1 = MakeTransaction({"A"}, {"B", "C"});
  txn1->mutable_internal()->set_id(100);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);
  auto txn2 = MakeTransaction({"A"}, {"B"});
  txn2->mutable_internal()->set_id(200);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);
  auto txn3 = MakeTransaction({}, {"A", "C"});
  txn3->mutable_internal()->set_id(300);
  TransactionHolder holder3 = MakeHolder(configs[0], txn3);

  ASSERT_TRUE(lock_manager.AcceptTransactionAndAcquireLocks(holder1));
  ASSERT_FALSE(lock_manager.AcceptTransactionAndAcquireLocks(holder2));
  ASSERT_FALSE(lock_manager.AcceptTransactionAndAcquireLocks(holder3));

  auto new_ready_txns = lock_manager.ReleaseLocks(holder1);
  ASSERT_EQ(new_ready_txns.size(), 1U);
  ASSERT_TRUE(new_ready_txns.count(200) > 0);

  new_ready_txns = lock_manager.ReleaseLocks(holder2);
  ASSERT_EQ(new_ready_txns.size(), 1U);
  ASSERT_TRUE(new_ready_txns.count(300) > 0);
}

TEST(LockManagerDeprecated, PrioritizeWriteLock) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  LockManagerDeprecated lock_manager;
  auto txn1 = MakeTransaction({"A"}, {"A"});
  txn1->mutable_internal()->set_id(100);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);
  auto txn2 = MakeTransaction({"A"}, {});
  txn2->mutable_internal()->set_id(200);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);
 
  ASSERT_TRUE(lock_manager.AcceptTransactionAndAcquireLocks(holder1));
  ASSERT_FALSE(lock_manager.AcceptTransactionAndAcquireLocks(holder2));

  auto new_ready_txns = lock_manager.ReleaseLocks(holder1);
  ASSERT_EQ(new_ready_txns.size(), 1U);
  ASSERT_TRUE(new_ready_txns.count(200) > 0);
}

TEST(LockManagerDeprecated, AcquireLocksWithLockOnlyTxn1) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  LockManagerDeprecated lock_manager;
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
  ASSERT_FALSE(lock_manager.AcquireLocks(holder2_lockonly1));
  ASSERT_FALSE(lock_manager.AcquireLocks(holder1));
  ASSERT_TRUE(lock_manager.AcquireLocks(holder2_lockonly2));

  auto new_ready_txns = lock_manager.ReleaseLocks(holder2);
  ASSERT_EQ(new_ready_txns.size(), 1U);
  ASSERT_TRUE(new_ready_txns.count(100) > 0);
}

TEST(LockManagerDeprecated, AcquireLocksWithLockOnlyTxn2) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  LockManagerDeprecated lock_manager;
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

  ASSERT_FALSE(lock_manager.AcquireLocks(holder2_lockonly1));
  ASSERT_FALSE(lock_manager.AcquireLocks(holder1));
  ASSERT_FALSE(lock_manager.AcquireLocks(holder2_lockonly2));
  ASSERT_FALSE(lock_manager.AcceptTransaction(holder1));
  ASSERT_TRUE(lock_manager.AcceptTransaction(holder2));

  auto new_ready_txns = lock_manager.ReleaseLocks(holder2);
  ASSERT_EQ(new_ready_txns.size(), 1U);
  ASSERT_TRUE(new_ready_txns.count(100) > 0);
}

TEST(LockManagerDeprecated, AcquireLocksWithLockOnlyTxnOutOfOrder) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  LockManagerDeprecated lock_manager;
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

  ASSERT_FALSE(lock_manager.AcquireLocks(holder2_lockonly1));
  ASSERT_FALSE(lock_manager.AcceptTransaction(holder2));
  ASSERT_FALSE(lock_manager.AcquireLocks(holder1));
  ASSERT_FALSE(lock_manager.AcceptTransaction(holder1));
  ASSERT_TRUE(lock_manager.AcquireLocks(holder2_lockonly2));

  auto new_ready_txns = lock_manager.ReleaseLocks(holder2);
  ASSERT_EQ(new_ready_txns.size(), 1U);
  ASSERT_TRUE(new_ready_txns.count(100) > 0);
}

TEST(LockManagerDeprecated, GhostTxns) {
  auto configs = MakeTestConfigurations("locking", 1, 2);
  LockManagerDeprecated lock_manager;
  // "X" is in partition 1
  auto txn1 = MakeTransaction({}, {"X"});
  txn1->mutable_internal()->set_id(100);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);
  ASSERT_FALSE(lock_manager.AcceptTransaction(holder1));

  // "Z" is in partition 1
  auto txn2 = MakeTransaction({"Z"}, {});
  txn2->mutable_internal()->set_id(101);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);
  ASSERT_FALSE(lock_manager.AcquireLocks(holder2));
}

TEST(LockManagerDeprecated, BlockedLockOnlyTxn) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  LockManagerDeprecated lock_manager;

  auto txn1 = MakeTransaction({"A"}, {"B"});
  txn1->mutable_internal()->set_id(100);
  TransactionHolder holder1 = MakeHolder(configs[0], txn1);
  ASSERT_TRUE(lock_manager.AcceptTransactionAndAcquireLocks(holder1));

  auto txn2 = MakeTransaction({}, {"B"});
  txn2->mutable_internal()->set_id(101);
  TransactionHolder holder2 = MakeHolder(configs[0], txn2);
  ASSERT_FALSE(lock_manager.AcquireLocks(holder2));
}