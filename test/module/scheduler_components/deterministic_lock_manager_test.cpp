#include <gtest/gtest.h>

#include "common/test_utils.h"
#include "common/proto_utils.h"
#include "module/scheduler_components/deterministic_lock_manager.h"

using namespace std;
using namespace slog;

class DeterministicLockManagerTest : public ::testing::Test {
public:
  DeterministicLockManagerTest() :
      storage(make_shared<slog::MemOnlyStorage<Key, Record, Metadata>>()),
      lock_manager(storage) {
        LOG(INFO) << storage;
      }
  shared_ptr<Storage<Key, Record>> storage;
  DeterministicLockManager lock_manager;
};

TEST_F(DeterministicLockManagerTest, GetAllLocksOnFirstTry) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn = FillEmptyMetadata(MakeTransaction(
    {"readA", "readB"},
    {"writeC"}));
  TransactionHolder holder(configs[0], txn);
  ASSERT_EQ(lock_manager.AcceptTransactionAndAcquireLocks(holder), AcquireLocksResult::ACQUIRED);
  auto result = lock_manager.ReleaseLocks(holder);
  ASSERT_TRUE(result.empty());
}

TEST_F(DeterministicLockManagerTest, GetAllLocksMultiPartitions) {
  auto configs = MakeTestConfigurations("locking", 1, 2);
  // "AAAA" is in partition 0 so lock is acquired
  auto txn1 = FillEmptyMetadata(MakeTransaction({"readX"}, {"AAAA"}));
  txn1->mutable_internal()->set_id(100);
  TransactionHolder holder1(configs[0], txn1);
  // "ZZZZ" is in partition 1 so is ignored
  auto txn2 = FillEmptyMetadata(MakeTransaction({"readX"}, {"ZZZZ"}));
  txn2->mutable_internal()->set_id(200);
  TransactionHolder holder2(configs[0], txn2);
  ASSERT_EQ(lock_manager.AcceptTransactionAndAcquireLocks(holder1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTransactionAndAcquireLocks(holder2), AcquireLocksResult::WAITING);
}

TEST_F(DeterministicLockManagerTest, ReadLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = FillEmptyMetadata(MakeTransaction({"readA", "readB"}, {}));
  txn1->mutable_internal()->set_id(100);
  TransactionHolder holder1(configs[0], txn1);

  auto txn2 = FillEmptyMetadata(MakeTransaction({"readB", "readC"}, {}));
  txn2->mutable_internal()->set_id(200);
  TransactionHolder holder2(configs[0], txn2);

  ASSERT_EQ(lock_manager.AcceptTransactionAndAcquireLocks(holder1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTransactionAndAcquireLocks(holder2), AcquireLocksResult::WAITING);
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder1).empty());
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder2).empty());
}

TEST_F(DeterministicLockManagerTest, WriteLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = FillEmptyMetadata(MakeTransaction({}, {"writeA", "writeB"}));
  txn1->mutable_internal()->set_id(100);
  TransactionHolder holder1(configs[0], txn1);

  auto txn2 = FillEmptyMetadata(MakeTransaction({"readA"}, {"writeA"}));
  txn2->mutable_internal()->set_id(200);
  TransactionHolder holder2(configs[0], txn2);

  ASSERT_EQ(lock_manager.AcceptTransactionAndAcquireLocks(holder1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTransactionAndAcquireLocks(holder2), AcquireLocksResult::WAITING);
  // The blocked txn becomes ready
  ASSERT_EQ(lock_manager.ReleaseLocks(holder1).size(), 1U);
  // Make sure the lock is already held by txn2
  ASSERT_EQ(lock_manager.AcceptTransactionAndAcquireLocks(holder1), AcquireLocksResult::WAITING);
}

TEST_F(DeterministicLockManagerTest, ReleaseLocksAndGetManyNewHolders) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = FillEmptyMetadata(MakeTransaction({"A"}, {"B", "C"}));
  txn1->mutable_internal()->set_id(100);
  TransactionHolder holder1(configs[0], txn1);
  auto txn2 = FillEmptyMetadata(MakeTransaction({"B"}, {"A"}));
  txn2->mutable_internal()->set_id(200);
  TransactionHolder holder2(configs[0], txn2);
  auto txn3 = FillEmptyMetadata(MakeTransaction({"B"}, {}));
  txn3->mutable_internal()->set_id(300);
  TransactionHolder holder3(configs[0], txn3);
  auto txn4 = FillEmptyMetadata(MakeTransaction({"C"}, {}));
  txn4->mutable_internal()->set_id(400);
  TransactionHolder holder4(configs[0], txn4);

  ASSERT_EQ(lock_manager.AcceptTransactionAndAcquireLocks(holder1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTransactionAndAcquireLocks(holder2), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTransactionAndAcquireLocks(holder3), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTransactionAndAcquireLocks(holder4), AcquireLocksResult::WAITING);

  ASSERT_TRUE(lock_manager.ReleaseLocks(holder3).empty());

  auto result = lock_manager.ReleaseLocks(holder1);
  // Txn 300 was removed from the wait list due to the
  // ReleaseLocks call above
  ASSERT_EQ(result.size(), 2U);
  ASSERT_TRUE(result.count(200) > 0);
  ASSERT_TRUE(result.count(400) > 0);
}

TEST_F(DeterministicLockManagerTest, PartiallyAcquiredLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = FillEmptyMetadata(MakeTransaction({"A"}, {"B", "C"}));
  txn1->mutable_internal()->set_id(100);
  TransactionHolder holder1(configs[0], txn1);
  auto txn2 = FillEmptyMetadata(MakeTransaction({"A"}, {"B"}));
  txn2->mutable_internal()->set_id(200);
  TransactionHolder holder2(configs[0], txn2);
  auto txn3 = FillEmptyMetadata(MakeTransaction({}, {"A", "C"}));
  txn3->mutable_internal()->set_id(300);
  TransactionHolder holder3(configs[0], txn3);

  ASSERT_EQ(lock_manager.AcceptTransactionAndAcquireLocks(holder1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTransactionAndAcquireLocks(holder2), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTransactionAndAcquireLocks(holder3), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(holder1);
  ASSERT_EQ(result.size(), 1U);
  ASSERT_TRUE(result.count(200) > 0);

  result = lock_manager.ReleaseLocks(holder2);
  ASSERT_EQ(result.size(), 1U);
  ASSERT_TRUE(result.count(300) > 0);
}

TEST_F(DeterministicLockManagerTest, PrioritizeWriteLock) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = FillEmptyMetadata(MakeTransaction({"A"}, {"A"}));
  txn1->mutable_internal()->set_id(100);
  TransactionHolder holder1(configs[0], txn1);
  auto txn2 = FillEmptyMetadata(MakeTransaction({"A"}, {}));
  txn2->mutable_internal()->set_id(200);
  TransactionHolder holder2(configs[0], txn2);
 
  ASSERT_EQ(lock_manager.AcceptTransactionAndAcquireLocks(holder1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTransactionAndAcquireLocks(holder2), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(holder1);
  ASSERT_EQ(result.size(), 1U);
  ASSERT_TRUE(result.count(200) > 0);
}

TEST_F(DeterministicLockManagerTest, AcquireLocksWithLockOnlyTxn1) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = FillEmptyMetadata(MakeTransaction({"A"}, {"B", "C"}));
  txn1->mutable_internal()->set_id(100);
  TransactionHolder holder1(configs[0], txn1);
  auto txn2 = FillEmptyMetadata(MakeTransaction({"A"}, {"B"}));
  txn2->mutable_internal()->set_id(200);
  TransactionHolder holder2(configs[0], txn2);
  auto txn2_lockonly1 = FillEmptyMetadata(MakeTransaction({}, {"B"}));
  txn2_lockonly1->mutable_internal()->set_id(200);
  TransactionHolder holder2_lockonly1(configs[0], txn2_lockonly1);
  auto txn2_lockonly2 = FillEmptyMetadata(MakeTransaction({"A"}, {}));
  txn2_lockonly2->mutable_internal()->set_id(200);
  TransactionHolder holder2_lockonly2(configs[0], txn2_lockonly2);

  ASSERT_FALSE(lock_manager.AcceptTransaction(holder1));
  ASSERT_FALSE(lock_manager.AcceptTransaction(holder2));
  ASSERT_EQ(lock_manager.AcquireLocks(holder2_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2_lockonly2), AcquireLocksResult::ACQUIRED);

  auto result = lock_manager.ReleaseLocks(holder2);
  ASSERT_EQ(result.size(), 1U);
  ASSERT_TRUE(result.count(100) > 0);
}

TEST_F(DeterministicLockManagerTest, AcquireLocksWithLockOnlyTxn2) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = FillEmptyMetadata(MakeTransaction({"A"}, {"B", "C"}));
  txn1->mutable_internal()->set_id(100);
  TransactionHolder holder1(configs[0], txn1);
  auto txn2 = FillEmptyMetadata(MakeTransaction({"A"}, {"B"}));
  txn2->mutable_internal()->set_id(200);
  TransactionHolder holder2(configs[0], txn2);
  auto txn2_lockonly1 = FillEmptyMetadata(MakeTransaction({}, {"B"}));
  txn2_lockonly1->mutable_internal()->set_id(200);
  TransactionHolder holder2_lockonly1(configs[0], txn2_lockonly1);
  auto txn2_lockonly2 = FillEmptyMetadata(MakeTransaction({"A"}, {}));
  txn2_lockonly2->mutable_internal()->set_id(200);
  TransactionHolder holder2_lockonly2(configs[0], txn2_lockonly2);

  ASSERT_EQ(lock_manager.AcquireLocks(holder2_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2_lockonly2), AcquireLocksResult::WAITING);
  ASSERT_FALSE(lock_manager.AcceptTransaction(holder1));
  ASSERT_TRUE(lock_manager.AcceptTransaction(holder2));

  auto result = lock_manager.ReleaseLocks(holder2);
  ASSERT_EQ(result.size(), 1U);
  ASSERT_TRUE(result.count(100) > 0);
}

TEST_F(DeterministicLockManagerTest, AcquireLocksWithLockOnlyTxnOutOfOrder) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = FillEmptyMetadata(MakeTransaction({"A"}, {"B", "C"}));
  txn1->mutable_internal()->set_id(100);
  TransactionHolder holder1(configs[0], txn1);
  auto txn2 = FillEmptyMetadata(MakeTransaction({"A"}, {"B"}));
  txn2->mutable_internal()->set_id(200);
  TransactionHolder holder2(configs[0], txn2);
  auto txn2_lockonly1 = FillEmptyMetadata(MakeTransaction({}, {"B"}));
  txn2_lockonly1->mutable_internal()->set_id(200);
  TransactionHolder holder2_lockonly1(configs[0], txn2_lockonly1);
  auto txn2_lockonly2 = FillEmptyMetadata(MakeTransaction({"A"}, {}));
  txn2_lockonly2->mutable_internal()->set_id(200);
  TransactionHolder holder2_lockonly2(configs[0], txn2_lockonly2);

  ASSERT_EQ(lock_manager.AcquireLocks(holder2_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_FALSE(lock_manager.AcceptTransaction(holder2));
  ASSERT_EQ(lock_manager.AcquireLocks(holder1), AcquireLocksResult::WAITING);
  ASSERT_FALSE(lock_manager.AcceptTransaction(holder1));
  ASSERT_EQ(lock_manager.AcquireLocks(holder2_lockonly2), AcquireLocksResult::ACQUIRED);

  auto result = lock_manager.ReleaseLocks(holder2);
  ASSERT_EQ(result.size(), 1U);
  ASSERT_TRUE(result.count(100) > 0);
}

// TODO: these txns shouldn't be in the system
// TEST_F(DeterministicLockManagerTest, GhostTxns) {
//   auto configs = MakeTestConfigurations("locking", 1, 2);
//   // "X" is in partition 1
//   auto txn1 = FillEmptyMetadata(MakeTransaction({}, {"X"}));
//   txn1->mutable_internal()->set_id(100);
//   TransactionHolder holder1(configs[0], txn1);
//   ASSERT_FALSE(lock_manager.AcceptTransaction(holder1));

//   // "Z" is in partition 1
//   auto txn2 = FillEmptyMetadata(MakeTransaction({"Z"}, {}));
//   txn2->mutable_internal()->set_id(101);
//   TransactionHolder holder2(configs[0], txn2);
//   ASSERT_EQ(lock_manager.AcquireLocks(holder2), AcquireLocksResult::WAITING);
// }

TEST_F(DeterministicLockManagerTest, BlockedLockOnlyTxn) {
  auto configs = MakeTestConfigurations("locking", 1, 1);

  auto txn1 = FillEmptyMetadata(MakeTransaction({"A"}, {"B"}));
  txn1->mutable_internal()->set_id(100);
  TransactionHolder holder1(configs[0], txn1);
  ASSERT_EQ(lock_manager.AcceptTransactionAndAcquireLocks(holder1), AcquireLocksResult::ACQUIRED);

  auto txn2 = FillEmptyMetadata(MakeTransaction({}, {"B"}));
  txn2->mutable_internal()->set_id(101);
  TransactionHolder holder2(configs[0], txn2);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2), AcquireLocksResult::WAITING);
}