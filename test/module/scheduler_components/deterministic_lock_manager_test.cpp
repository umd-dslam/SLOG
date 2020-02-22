#include <gmock/gmock.h>

#include "common/test_utils.h"
#include "common/proto_utils.h"
#include "module/scheduler_components/deterministic_lock_manager.h"

using namespace std;
using namespace slog;
using ::testing::ElementsAre;

class DeterministicLockManagerTest : public ::testing::Test {
protected:
  void SetUp() {
    storage = make_shared<slog::MemOnlyStorage<Key, Record, Metadata>>();
  }

  shared_ptr<Storage<Key, Record>> storage;
};

TEST_F(DeterministicLockManagerTest, GetAllLocksOnFirstTry) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  DeterministicLockManager lock_manager(configs[0], storage);
  auto txn = MakeTransaction(
    {"readA", "readB"},
    {"writeC"});
  ASSERT_TRUE(lock_manager.RegisterTxnAndAcquireLocks(txn));
  auto new_holders = lock_manager.ReleaseLocks(txn);
  ASSERT_TRUE(new_holders.empty());
}

TEST_F(DeterministicLockManagerTest, GetAllLocksMultiPartitions) {
  auto configs = MakeTestConfigurations("locking", 1, 2);
  DeterministicLockManager lock_manager(configs[0], storage);
  // "AAAA" is in partition 0 so lock is acquired
  auto txn1 = MakeTransaction({"readX"}, {"AAAA"});
  txn1.mutable_internal()->set_id(100);
  // "ZZZZ" is in partition 1 so is ignored
  auto txn2 = MakeTransaction({"readX"}, {"ZZZZ"});
  txn2.mutable_internal()->set_id(200);
  ASSERT_TRUE(lock_manager.RegisterTxnAndAcquireLocks(txn1));
  ASSERT_FALSE(lock_manager.RegisterTxnAndAcquireLocks(txn2));
}

TEST_F(DeterministicLockManagerTest, ReadLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  DeterministicLockManager lock_manager(configs[0], storage);
  auto txn1 = MakeTransaction({"readA", "readB"}, {});
  txn1.mutable_internal()->set_id(100);
  auto txn2 = MakeTransaction({"readB", "readC"}, {});
  txn2.mutable_internal()->set_id(200);
  ASSERT_TRUE(lock_manager.RegisterTxnAndAcquireLocks(txn1));
  ASSERT_TRUE(lock_manager.RegisterTxnAndAcquireLocks(txn2));
  ASSERT_TRUE(lock_manager.ReleaseLocks(txn1).empty());
  ASSERT_TRUE(lock_manager.ReleaseLocks(txn2).empty());
}

TEST_F(DeterministicLockManagerTest, WriteLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  DeterministicLockManager lock_manager(configs[0], storage);
  auto txn1 = MakeTransaction({}, {"writeA", "writeB"});
  txn1.mutable_internal()->set_id(100);
  auto txn2 = MakeTransaction({"readA"}, {"writeA"});
  txn2.mutable_internal()->set_id(200);
  ASSERT_TRUE(lock_manager.RegisterTxnAndAcquireLocks(txn1));
  ASSERT_FALSE(lock_manager.RegisterTxnAndAcquireLocks(txn2));
  // The blocked txn becomes ready
  ASSERT_EQ(lock_manager.ReleaseLocks(txn1).size(), 1);
  // Make sure the lock is already held by txn2
  ASSERT_FALSE(lock_manager.RegisterTxnAndAcquireLocks(txn1));
}

TEST_F(DeterministicLockManagerTest, ReleaseLocksAndGetManyNewHolders) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  DeterministicLockManager lock_manager(configs[0], storage);
  auto txn1 = MakeTransaction({"A"}, {"B", "C"});
  txn1.mutable_internal()->set_id(100);
  auto txn2 = MakeTransaction({"B"}, {"A"});
  txn2.mutable_internal()->set_id(200);
  auto txn3 = MakeTransaction({"B"}, {});
  txn3.mutable_internal()->set_id(300);
  auto txn4 = MakeTransaction({"C"}, {});
  txn4.mutable_internal()->set_id(400);

  ASSERT_TRUE(lock_manager.RegisterTxnAndAcquireLocks(txn1));
  ASSERT_FALSE(lock_manager.RegisterTxnAndAcquireLocks(txn2));
  ASSERT_FALSE(lock_manager.RegisterTxnAndAcquireLocks(txn3));
  ASSERT_FALSE(lock_manager.RegisterTxnAndAcquireLocks(txn4));

  ASSERT_TRUE(lock_manager.ReleaseLocks(txn3).empty());

  auto new_ready_txns = lock_manager.ReleaseLocks(txn1);
  // Txn 300 was removed from the wait list due to the
  // ReleaseLocks call above
  ASSERT_EQ(new_ready_txns.size(), 2);
  ASSERT_TRUE(new_ready_txns.count(200) > 0);
  ASSERT_TRUE(new_ready_txns.count(400) > 0);
}

TEST_F(DeterministicLockManagerTest, PartiallyAcquiredLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  DeterministicLockManager lock_manager(configs[0], storage);
  auto txn1 = MakeTransaction({"A"}, {"B", "C"});
  txn1.mutable_internal()->set_id(100);
  auto txn2 = MakeTransaction({"A"}, {"B"});
  txn2.mutable_internal()->set_id(200);
  auto txn3 = MakeTransaction({}, {"A", "C"});
  txn3.mutable_internal()->set_id(300);

  ASSERT_TRUE(lock_manager.RegisterTxnAndAcquireLocks(txn1));
  ASSERT_FALSE(lock_manager.RegisterTxnAndAcquireLocks(txn2));
  ASSERT_FALSE(lock_manager.RegisterTxnAndAcquireLocks(txn3));

  auto new_ready_txns = lock_manager.ReleaseLocks(txn1);
  ASSERT_EQ(new_ready_txns.size(), 1);
  ASSERT_TRUE(new_ready_txns.count(200) > 0);

  new_ready_txns = lock_manager.ReleaseLocks(txn2);
  ASSERT_EQ(new_ready_txns.size(), 1);
  ASSERT_TRUE(new_ready_txns.count(300) > 0);
}

TEST_F(DeterministicLockManagerTest, PrioritizeWriteLock) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  DeterministicLockManager lock_manager(configs[0], storage);
  auto txn1 = MakeTransaction({"A"}, {"A"});
  txn1.mutable_internal()->set_id(100);
  auto txn2 = MakeTransaction({"A"}, {});
  txn2.mutable_internal()->set_id(200);
 
  ASSERT_TRUE(lock_manager.RegisterTxnAndAcquireLocks(txn1));
  ASSERT_FALSE(lock_manager.RegisterTxnAndAcquireLocks(txn2));

  auto new_ready_txns = lock_manager.ReleaseLocks(txn1);
  ASSERT_EQ(new_ready_txns.size(), 1);
  ASSERT_TRUE(new_ready_txns.count(200) > 0);
}

TEST_F(DeterministicLockManagerTest, AcquireLocksWithLockOnlyTxn1) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  DeterministicLockManager lock_manager(configs[0], storage);
  auto txn1 = MakeTransaction({"A"}, {"B", "C"});
  txn1.mutable_internal()->set_id(100);
  auto txn2 = MakeTransaction({"A"}, {"B"});
  txn2.mutable_internal()->set_id(200);
  auto txn2_lockonly1 = MakeTransaction({}, {"B"});
  txn2_lockonly1.mutable_internal()->set_id(200);
  auto txn2_lockonly2 = MakeTransaction({"A"}, {});
  txn2_lockonly2.mutable_internal()->set_id(200);

  ASSERT_FALSE(lock_manager.RegisterTxn(txn1));
  ASSERT_FALSE(lock_manager.RegisterTxn(txn2));
  ASSERT_FALSE(lock_manager.AcquireLocks(txn2_lockonly1));
  ASSERT_FALSE(lock_manager.AcquireLocks(txn1));
  ASSERT_TRUE(lock_manager.AcquireLocks(txn2_lockonly2));

  auto new_ready_txns = lock_manager.ReleaseLocks(txn2);
  ASSERT_EQ(new_ready_txns.size(), 1);
  ASSERT_TRUE(new_ready_txns.count(100) > 0);
}

TEST_F(DeterministicLockManagerTest, AcquireLocksWithLockOnlyTxn2) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  DeterministicLockManager lock_manager(configs[0], storage);
  auto txn1 = MakeTransaction({"A"}, {"B", "C"});
  txn1.mutable_internal()->set_id(100);
  auto txn2 = MakeTransaction({"A"}, {"B"});
  txn2.mutable_internal()->set_id(200);
  auto txn2_lockonly1 = MakeTransaction({}, {"B"});
  txn2_lockonly1.mutable_internal()->set_id(200);
  auto txn2_lockonly2 = MakeTransaction({"A"}, {});
  txn2_lockonly2.mutable_internal()->set_id(200);

  ASSERT_FALSE(lock_manager.AcquireLocks(txn2_lockonly1));
  ASSERT_FALSE(lock_manager.AcquireLocks(txn1));
  ASSERT_FALSE(lock_manager.AcquireLocks(txn2_lockonly2));
  ASSERT_FALSE(lock_manager.RegisterTxn(txn1));
  ASSERT_TRUE(lock_manager.RegisterTxn(txn2));

  auto new_ready_txns = lock_manager.ReleaseLocks(txn2);
  ASSERT_EQ(new_ready_txns.size(), 1);
  ASSERT_TRUE(new_ready_txns.count(100) > 0);
}

TEST_F(DeterministicLockManagerTest, AcquireLocksWithLockOnlyTxnOutOfOrder) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  DeterministicLockManager lock_manager(configs[0], storage);
  auto txn1 = MakeTransaction({"A"}, {"B", "C"});
  txn1.mutable_internal()->set_id(100);
  auto txn2 = MakeTransaction({"A"}, {"B"});
  txn2.mutable_internal()->set_id(200);
  auto txn2_lockonly1 = MakeTransaction({}, {"B"});
  txn2_lockonly1.mutable_internal()->set_id(200);
  auto txn2_lockonly2 = MakeTransaction({"A"}, {});
  txn2_lockonly2.mutable_internal()->set_id(200);

  ASSERT_FALSE(lock_manager.AcquireLocks(txn2_lockonly1));
  ASSERT_FALSE(lock_manager.RegisterTxn(txn2));
  ASSERT_FALSE(lock_manager.AcquireLocks(txn1));
  ASSERT_FALSE(lock_manager.RegisterTxn(txn1));
  ASSERT_TRUE(lock_manager.AcquireLocks(txn2_lockonly2));

  auto new_ready_txns = lock_manager.ReleaseLocks(txn2);
  ASSERT_EQ(new_ready_txns.size(), 1);
  ASSERT_TRUE(new_ready_txns.count(100) > 0);
}

TEST_F(DeterministicLockManagerTest, GhostTxns) {
  auto configs = MakeTestConfigurations("locking", 1, 2);
  DeterministicLockManager lock_manager(configs[0], storage);
  // "X" is in partition 1
  auto txn1 = MakeTransaction({}, {"X"});
  txn1.mutable_internal()->set_id(100);
  ASSERT_FALSE(lock_manager.RegisterTxn(txn1));

  // "Z" is in partition 1
  auto txn2 = MakeTransaction({"Z"}, {});
  txn2.mutable_internal()->set_id(101);
  ASSERT_FALSE(lock_manager.AcquireLocks(txn2));
}

//////// REMASTERING ////////

TEST_F(DeterministicLockManagerTest, CheckCounters) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  DeterministicLockManager lock_manager(configs[0], storage);
  storage->Write("A", Record("valueA", 0, 1));
  auto txn1 = MakeTransaction({"A"}, {}, "some code", {{"A", {0, 1}}});
  auto txn2 = MakeTransaction({"A"}, {}, "some code", {{"A", {0, 2}}});
  auto txn3 = MakeTransaction({"A"}, {}, "some code", {{"A", {0, 0}}});

  ASSERT_EQ(lock_manager.VerifyMaster(txn1), VerifyMasterResult::Valid);
  ASSERT_EQ(lock_manager.VerifyMaster(txn2), VerifyMasterResult::Waiting);
  ASSERT_EQ(lock_manager.VerifyMaster(txn3), VerifyMasterResult::Aborted);
}

TEST_F(DeterministicLockManagerTest, CheckMultipleCounters) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  DeterministicLockManager lock_manager(configs[0], storage);
  storage->Write("A", Record("valueA", 0, 1));
  storage->Write("B", Record("valueB", 0, 1));
  storage->Write("C", Record("valueC", 0, 1));
  auto txn1 = MakeTransaction({"A", "C"}, {"B"}, "some code", {{"A", {0, 1}}, {"B", {0, 1}}, {"C", {0, 1}}});
  auto txn2 = MakeTransaction({"A", "C"}, {"B"}, "some code", {{"A", {0, 1}}, {"B", {0, 1}}, {"C", {0, 2}}});
  auto txn3 = MakeTransaction({"A", "C"}, {"B"}, "some code", {{"A", {0, 1}}, {"B", {0, 0}}, {"C", {0, 2}}});

  ASSERT_EQ(lock_manager.VerifyMaster(txn1), VerifyMasterResult::Valid);
  ASSERT_EQ(lock_manager.VerifyMaster(txn2), VerifyMasterResult::Waiting);
  ASSERT_EQ(lock_manager.VerifyMaster(txn3), VerifyMasterResult::Aborted);
}

TEST_F(DeterministicLockManagerTest, RemasterQueueSingleKey) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  DeterministicLockManager lock_manager(configs[0], storage);
  storage->Write("A", Record("valueA", 0, 1));
  auto txn1 = MakeTransaction({"A"}, {}, "some code", {{"A", {0, 2}}});
  txn1.mutable_internal()->set_id(100);

  ASSERT_EQ(lock_manager.VerifyMaster(txn1), VerifyMasterResult::Waiting);
  auto unblocked = lock_manager.RemasterOccured("A");
  ASSERT_THAT(unblocked, ElementsAre(100));
}

TEST_F(DeterministicLockManagerTest, RemasterQueueMultipleKeys) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  DeterministicLockManager lock_manager(configs[0], storage);
  storage->Write("A", Record("valueA", 0, 1));
  storage->Write("B", Record("valueB", 0, 1));
  auto txn1 = MakeTransaction({"A"}, {"B"}, "some code", {{"A", {0, 2}}, {"B", {0, 3}}});
  txn1.mutable_internal()->set_id(100);

  ASSERT_EQ(lock_manager.VerifyMaster(txn1), VerifyMasterResult::Waiting);
  ASSERT_EQ(lock_manager.RemasterOccured("B").size(), 0);
  ASSERT_EQ(lock_manager.RemasterOccured("A").size(), 0);
  auto unblocked = lock_manager.RemasterOccured("B");
  ASSERT_THAT(unblocked, ElementsAre(100));
}

TEST_F(DeterministicLockManagerTest, RemasterQueueMultipleTxns) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  DeterministicLockManager lock_manager(configs[0], storage);
  storage->Write("A", Record("valueA", 0, 1));
  storage->Write("B", Record("valueB", 0, 1));
  auto txn1 = MakeTransaction({"A"}, {"B"}, "some code", {{"A", {0, 2}}, {"B", {0, 1}}});
  txn1.mutable_internal()->set_id(100);
  auto txn2 = MakeTransaction({"A"}, {"B"}, "some code", {{"A", {0, 2}}, {"B", {0, 2}}});
  txn2.mutable_internal()->set_id(101);

  ASSERT_EQ(lock_manager.VerifyMaster(txn1), VerifyMasterResult::Waiting);
  ASSERT_EQ(lock_manager.VerifyMaster(txn2), VerifyMasterResult::Waiting);
  ASSERT_EQ(lock_manager.RemasterOccured("B").size(), 0);
  auto unblocked = lock_manager.RemasterOccured("A");
  ASSERT_THAT(unblocked, ElementsAre(100, 101));
}
