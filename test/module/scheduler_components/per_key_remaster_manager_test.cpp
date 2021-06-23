#include "module/scheduler_components/per_key_remaster_manager.h"

#include <gmock/gmock.h>

#include "common/proto_utils.h"
#include "test/test_utils.h"

using namespace std;
using namespace slog;
using std::make_pair;
using ::testing::ElementsAre;
using ::testing::IsEmpty;

class PerKeyRemasterManagerTest : public ::testing::Test {
 protected:
  void SetUp() {
    configs = MakeTestConfigurations("locking", 2, 1);
    storage = make_shared<slog::MemOnlyStorage>();
    remaster_manager = make_unique<PerKeyRemasterManager>(storage);
  }

  ConfigVec configs;
  shared_ptr<Storage> storage;
  unique_ptr<RemasterManager> remaster_manager;
};

TEST_F(PerKeyRemasterManagerTest, CheckCounters) {
  storage->Write("A", Record("valueA", 0, 1));
  auto txn1 = MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::READ, {{0, 1}}}});
  auto txn2 = MakeTestTxnHolder(configs[0], 200, {{"A", KeyType::READ, {{0, 2}}}});
  auto txn3 = MakeTestTxnHolder(configs[0], 300, {{"A", KeyType::READ, {{0, 0}}}});

  ASSERT_EQ(remaster_manager->VerifyMaster(txn1.lock_only_txn(0)), VerifyMasterResult::VALID);
  ASSERT_EQ(remaster_manager->VerifyMaster(txn2.lock_only_txn(0)), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(txn3.lock_only_txn(0)), VerifyMasterResult::ABORT);
}

TEST_F(PerKeyRemasterManagerTest, CheckMultipleCounters) {
  storage->Write("A", Record("valueA", 0, 1));
  storage->Write("B", Record("valueB", 0, 1));
  storage->Write("C", Record("valueC", 0, 1));
  auto txn1 = MakeTestTxnHolder(
      configs[0], 100,
      {{"A", KeyType::READ, {{0, 1}}}, {"C", KeyType::READ, {{0, 1}}}, {"B", KeyType::WRITE, {{0, 1}}}});
  auto txn2 = MakeTestTxnHolder(
      configs[0], 200,
      {{"A", KeyType::READ, {{0, 1}}}, {"C", KeyType::READ, {{0, 2}}}, {"B", KeyType::WRITE, {{0, 1}}}});
  auto txn3 = MakeTestTxnHolder(
      configs[0], 300,
      {{"A", KeyType::READ, {{0, 1}}}, {"C", KeyType::READ, {{0, 2}}}, {"B", KeyType::WRITE, {{0, 0}}}});

  ASSERT_EQ(remaster_manager->VerifyMaster(txn1.lock_only_txn(0)), VerifyMasterResult::VALID);
  ASSERT_EQ(remaster_manager->VerifyMaster(txn2.lock_only_txn(0)), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(txn3.lock_only_txn(0)), VerifyMasterResult::ABORT);
}

TEST_F(PerKeyRemasterManagerTest, CheckIndirectBlocking) {
  storage->Write("A", Record("valueA", 0, 1));
  storage->Write("B", Record("valueB", 0, 1));
  auto txn1 = MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::READ, {{0, 1}}}, {"B", KeyType::WRITE, {{0, 2}}}});
  auto txn2 = MakeTestTxnHolder(configs[0], 200, {{"A", KeyType::READ, {{0, 1}}}});
  auto txn3 = MakeTestTxnHolder(configs[0], 300, {{"B", KeyType::READ, {{0, 1}}}});

  ASSERT_EQ(remaster_manager->VerifyMaster(txn1.lock_only_txn(0)), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(txn2.lock_only_txn(0)), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(txn3.lock_only_txn(0)), VerifyMasterResult::VALID);
}

TEST_F(PerKeyRemasterManagerTest, RemasterQueueSingleKey) {
  storage->Write("A", Record("valueA", 0, 1));
  auto txn1 = MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::READ, {{1, 2}}}});

  ASSERT_EQ(remaster_manager->VerifyMaster(txn1.lock_only_txn(1)), VerifyMasterResult::WAITING);
  storage->Write("A", Record("valueA", 1, 2));
  auto unblocked = remaster_manager->RemasterOccured("A", 2).unblocked;
  ASSERT_THAT(unblocked, ElementsAre(&txn1.lock_only_txn(1)));
}

TEST_F(PerKeyRemasterManagerTest, RemasterQueueMultipleKeys) {
  storage->Write("A", Record("valueA", 0, 1));
  storage->Write("B", Record("valueB", 1, 1));
  auto txn1 = MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::READ, {{1, 2}}}, {"B", KeyType::WRITE, {{1, 3}}}});

  ASSERT_EQ(remaster_manager->VerifyMaster(txn1.lock_only_txn(1)), VerifyMasterResult::WAITING);
  storage->Write("B", Record("valueB", 0, 2));
  ASSERT_THAT(remaster_manager->RemasterOccured("B", 2).unblocked, ElementsAre());
  storage->Write("A", Record("valueA", 1, 2));
  ASSERT_THAT(remaster_manager->RemasterOccured("A", 2).unblocked, ElementsAre());
  storage->Write("B", Record("valueB", 1, 3));
  ASSERT_THAT(remaster_manager->RemasterOccured("B", 3).unblocked, ElementsAre(&txn1.lock_only_txn(1)));
}

TEST_F(PerKeyRemasterManagerTest, RemasterQueueMultipleTxns) {
  storage->Write("A", Record("valueA", 0, 1));
  storage->Write("B", Record("valueB", 0, 1));
  auto txn1 = MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::READ, {{1, 2}}}, {"B", KeyType::WRITE, {{1, 2}}}});
  auto txn2 = MakeTestTxnHolder(configs[0], 200, {{"B", KeyType::WRITE, {{1, 2}}}});

  ASSERT_EQ(remaster_manager->VerifyMaster(txn1.lock_only_txn(1)), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(txn2.lock_only_txn(1)), VerifyMasterResult::WAITING);
  storage->Write("B", Record("valueB", 1, 2));
  ASSERT_THAT(remaster_manager->RemasterOccured("B", 2).unblocked, ElementsAre());
  storage->Write("A", Record("valueA", 1, 2));
  ASSERT_THAT(remaster_manager->RemasterOccured("A", 2).unblocked,
              ElementsAre(&txn1.lock_only_txn(1), &txn2.lock_only_txn(1)));
}