#include <gmock/gmock.h>

#include "common/test_utils.h"
#include "common/proto_utils.h"
#include "module/scheduler_components/remaster_manager.h"

using namespace std;
using namespace slog;
using ::testing::ElementsAre;
using ::testing::IsEmpty;
using std::make_pair;

class RemasterManagerTest : public ::testing::Test {
protected:
  void SetUp() {
    configs = MakeTestConfigurations("locking", 1, 1);
    storage = make_shared<slog::MemOnlyStorage<Key, Record, Metadata>>();
    all_txns = make_shared<TransactionMap>();
    remaster_manager = make_unique<RemasterManager>(configs[0], storage, all_txns);
  }

  ConfigVec configs;
  shared_ptr<Storage<Key, Record>> storage;
  shared_ptr<TransactionMap> all_txns;
  unique_ptr<RemasterManager> remaster_manager;

  TransactionHolder& MakeHolder(Transaction txn, TxnId txn_id) {
    txn.mutable_internal()->set_id(txn_id);
    CHECK(all_txns->count(txn.internal().id()) == 0) << "Need to set txns to unique id's";
    auto keys = ExtractKeysInPartition(configs[0], txn);
    auto& holder = (*all_txns)[txn.internal().id()];
    holder.txn = make_unique<Transaction>(txn);
    holder.keys_in_partition = keys;
    return holder;
  }
};

TEST_F(RemasterManagerTest, CheckCounters) {
  storage->Write("A", Record("valueA", 0, 1));
  auto& txn1 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 1}}}), 100);
  auto& txn2 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 2}}}), 101);
  auto& txn3 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 0}}}), 102);

  ASSERT_EQ(remaster_manager->VerifyMaster(txn1), VerifyMasterResult::VALID);
  ASSERT_EQ(remaster_manager->VerifyMaster(txn2), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(txn3), VerifyMasterResult::ABORT);
}

TEST_F(RemasterManagerTest, CheckMultipleCounters) {
  storage->Write("A", Record("valueA", 0, 1));
  storage->Write("B", Record("valueB", 0, 1));
  storage->Write("C", Record("valueC", 0, 1));
  auto& txn1 = MakeHolder(MakeTransaction({"A", "C"}, {"B"}, "some code", {{"A", {0, 1}}, {"B", {0, 1}}, {"C", {0, 1}}}), 100);
  auto& txn2 = MakeHolder(MakeTransaction({"A", "C"}, {"B"}, "some code", {{"A", {0, 1}}, {"B", {0, 1}}, {"C", {0, 2}}}), 101);
  auto& txn3 = MakeHolder(MakeTransaction({"A", "C"}, {"B"}, "some code", {{"A", {0, 1}}, {"B", {0, 0}}, {"C", {0, 2}}}), 102);

  ASSERT_EQ(remaster_manager->VerifyMaster(txn1), VerifyMasterResult::VALID);
  ASSERT_EQ(remaster_manager->VerifyMaster(txn2), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(txn3), VerifyMasterResult::ABORT);
}

TEST_F(RemasterManagerTest, CheckIndirectBlocking) {
  storage->Write("A", Record("valueA", 0, 1));
  storage->Write("B", Record("valueB", 0, 1));
  auto& txn1 = MakeHolder(MakeTransaction({"A"}, {"B"}, "some code", {{"A", {0, 1}}, {"B", {0, 2}}}), 100);
  auto& txn2 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 1}}}), 101);
  auto& txn3 = MakeHolder(MakeTransaction({"B"}, {}, "some code", {{"B", {0, 1}}}), 102);

  ASSERT_EQ(remaster_manager->VerifyMaster(txn1), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(txn2), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(txn3), VerifyMasterResult::VALID);
}

TEST_F(RemasterManagerTest, RemasterQueueSingleKey) {
  storage->Write("A", Record("valueA", 0, 1));
  auto& txn1 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 2}}}), 100);

  ASSERT_EQ(remaster_manager->VerifyMaster(txn1), VerifyMasterResult::WAITING);
  auto unblocked = remaster_manager->RemasterOccured("A", 2);
  ASSERT_THAT(unblocked, ElementsAre(100));
}

TEST_F(RemasterManagerTest, RemasterQueueMultipleKeys) {
  storage->Write("A", Record("valueA", 0, 1));
  storage->Write("B", Record("valueB", 0, 1));
  auto& txn1 = MakeHolder(MakeTransaction({"A"}, {"B"}, "some code", {{"A", {0, 2}}, {"B", {0, 3}}}), 100);

  ASSERT_EQ(remaster_manager->VerifyMaster(txn1), VerifyMasterResult::WAITING);
  ASSERT_THAT(remaster_manager->RemasterOccured("B", 2), ElementsAre());
  ASSERT_THAT(remaster_manager->RemasterOccured("A", 2), ElementsAre());
  ASSERT_THAT(remaster_manager->RemasterOccured("B", 3), ElementsAre(100));
}

TEST_F(RemasterManagerTest, RemasterQueueMultipleTxns) {
  storage->Write("A", Record("valueA", 0, 1));
  storage->Write("B", Record("valueB", 0, 1));
  auto& txn1 = MakeHolder(MakeTransaction({"A"}, {"B"}, "some code", {{"A", {0, 2}}, {"B", {0, 1}}}), 100);
  auto& txn2 = MakeHolder(MakeTransaction({"A"}, {"B"}, "some code", {{"A", {0, 2}}, {"B", {0, 2}}}), 101);

  ASSERT_EQ(remaster_manager->VerifyMaster(txn1), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(txn2), VerifyMasterResult::WAITING);
  ASSERT_THAT(remaster_manager->RemasterOccured("B", 2), ElementsAre());
  ASSERT_THAT(remaster_manager->RemasterOccured("A", 2), ElementsAre(100, 101));
}

TEST_F(RemasterManagerTest, AvoidsDeadlock) {
  storage->Write("A", Record("valueA", 0, 1));
  storage->Write("B", Record("valueB", 0, 1));
  auto& txn1 = MakeHolder(MakeTransaction({"A"}, {"B"}, "some code", {{"A", {0, 2}}, {"B", {0, 1}}}), 101);
  auto& txn2 = MakeHolder(MakeTransaction({"A"}, {"B"}, "some code", {{"A", {0, 1}}, {"B", {0, 1}}}), 100);

  ASSERT_EQ(remaster_manager->VerifyMaster(txn1), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(txn2), VerifyMasterResult::VALID);
}