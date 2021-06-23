#include "module/scheduler_components/simple_remaster_manager.h"

#include <gmock/gmock.h>

#include "common/proto_utils.h"
#include "test/test_utils.h"

using namespace std;
using namespace slog;
using ::testing::ElementsAre;

class SimpleRemasterManagerTest : public ::testing::Test {
 protected:
  void SetUp() {
    configs = MakeTestConfigurations("remaster", 2, 1);
    storage = make_shared<slog::MemOnlyStorage>();
    remaster_manager = make_unique<SimpleRemasterManager>(storage);
  }

  ConfigVec configs;
  shared_ptr<Storage> storage;
  unique_ptr<SimpleRemasterManager> remaster_manager;
};

TEST_F(SimpleRemasterManagerTest, ValidateMetadata) {
  storage->Write("A", Record("value", 0, 1));
  auto txn = MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::READ, {{1, 1}}}});
  ASSERT_DEATH(remaster_manager->VerifyMaster(txn.lock_only_txn(1)), "Masters don't match");
}

TEST_F(SimpleRemasterManagerTest, CheckCounters) {
  storage->Write("A", Record("value", 0, 1));
  auto txn1 = MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::READ, {{0, 1}}}});
  auto txn2 = MakeTestTxnHolder(configs[0], 200, {{"A", KeyType::READ, {{0, 0}}}});
  auto txn3 = MakeTestTxnHolder(configs[0], 300, {{"A", KeyType::READ, {{0, 2}}}});

  ASSERT_EQ(remaster_manager->VerifyMaster(txn1.lock_only_txn(0)), VerifyMasterResult::VALID);
  ASSERT_EQ(remaster_manager->VerifyMaster(txn2.lock_only_txn(0)), VerifyMasterResult::ABORT);
  ASSERT_EQ(remaster_manager->VerifyMaster(txn3.lock_only_txn(0)), VerifyMasterResult::WAITING);
}

TEST_F(SimpleRemasterManagerTest, CheckMultipleCounters) {
  storage->Write("A", Record("value", 0, 1));
  storage->Write("B", Record("value", 0, 1));
  auto txn1 = MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::READ, {{0, 1}}}, {"B", KeyType::WRITE, {{0, 1}}}});
  auto txn2 = MakeTestTxnHolder(configs[0], 200, {{"A", KeyType::READ, {{0, 0}}}, {"B", KeyType::READ, {{0, 1}}}});
  auto txn3 = MakeTestTxnHolder(configs[0], 300, {{"A", KeyType::WRITE, {{0, 1}}}, {"B", KeyType::WRITE, {{0, 2}}}});

  ASSERT_EQ(remaster_manager->VerifyMaster(txn1.lock_only_txn(0)), VerifyMasterResult::VALID);
  ASSERT_EQ(remaster_manager->VerifyMaster(txn2.lock_only_txn(0)), VerifyMasterResult::ABORT);
  ASSERT_EQ(remaster_manager->VerifyMaster(txn3.lock_only_txn(0)), VerifyMasterResult::WAITING);
}

TEST_F(SimpleRemasterManagerTest, BlockLocalLog) {
  storage->Write("A", Record("value", 0, 1));
  storage->Write("B", Record("value", 1, 1));
  auto txn1 = MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::READ, {{0, 2}}}});
  auto txn2 = MakeTestTxnHolder(configs[0], 200, {{"A", KeyType::READ, {{0, 1}}}});
  auto txn3 = MakeTestTxnHolder(configs[0], 300, {{"B", KeyType::READ, {{1, 1}}}});
  ASSERT_EQ(remaster_manager->VerifyMaster(txn1.lock_only_txn(0)), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(txn2.lock_only_txn(0)), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(txn3.lock_only_txn(1)), VerifyMasterResult::VALID);
}

TEST_F(SimpleRemasterManagerTest, RemasterUnblocks) {
  storage->Write("A", Record("value", 0, 1));
  auto txn1 = MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::READ, {{0, 2}}}});
  auto txn2 = MakeTestTxnHolder(configs[0], 200, {{"A", KeyType::READ, {{0, 1}}}});

  ASSERT_EQ(remaster_manager->VerifyMaster(txn1.lock_only_txn(0)), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(txn2.lock_only_txn(0)), VerifyMasterResult::WAITING);

  storage->Write("A", Record("value", 0, 2));
  auto result = remaster_manager->RemasterOccured("A", 2);
  ASSERT_THAT(result.unblocked, ElementsAre(&txn1.lock_only_txn(0)));
  ASSERT_THAT(result.should_abort, ElementsAre(&txn2.lock_only_txn(0)));
}

TEST_F(SimpleRemasterManagerTest, ReleaseTransaction) {
  storage->Write("A", Record("value", 0, 1));
  storage->Write("B", Record("valueB", 1, 1));
  auto txn1 = MakeTestTxnHolder(configs[0], 100, {{"B", KeyType::READ, {{0, 2}}}});
  auto txn2 = MakeTestTxnHolder(configs[0], 200, {{"A", KeyType::READ, {{0, 1}}}});
  auto txn3 = MakeTestTxnHolder(configs[0], 300, {{"A", KeyType::READ, {{0, 1}}}});

  ASSERT_EQ(remaster_manager->VerifyMaster(txn1.lock_only_txn(0)), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(txn2.lock_only_txn(0)), VerifyMasterResult::WAITING);

  auto result = remaster_manager->ReleaseTransaction(txn3.txn());
  ASSERT_THAT(result.unblocked, ElementsAre());
  ASSERT_THAT(result.should_abort, ElementsAre());

  result = remaster_manager->ReleaseTransaction(txn1.txn());
  ASSERT_THAT(result.unblocked, ElementsAre(&txn2.lock_only_txn(0)));
  ASSERT_THAT(result.should_abort, ElementsAre());
}
