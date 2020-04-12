#include <gmock/gmock.h>

#include "common/test_utils.h"
#include "common/proto_utils.h"

#include "module/scheduler_components/simple_remaster_manager.h"

using namespace std;
using namespace slog;
using ::testing::ElementsAre;

class SimpleRemasterManagerTest : public ::testing::Test {
protected:
  void SetUp() {
    configs = MakeTestConfigurations("remaster", 2, 1);
    storage = make_shared<slog::MemOnlyStorage<Key, Record, Metadata>>();
    all_txns = make_shared<TransactionMap>();
    remaster_manager = make_unique<SimpleRemasterManager>(storage, all_txns);
  }

  ConfigVec configs;
  shared_ptr<Storage<Key, Record>> storage;
  shared_ptr<TransactionMap> all_txns;
  unique_ptr<RemasterManager> remaster_manager;

  TransactionHolder& MakeHolder(Transaction* txn, TxnId txn_id) {

    auto txn_replica_id = txn_id;
    txn->mutable_internal()->set_id(txn_id);
    txn->mutable_internal()->set_replica_id(txn_replica_id);
    CHECK(all_txns->count(txn->internal().id()) == 0) << "Need to set txns to unique id's";
    auto& holder = (*all_txns)[txn_replica_id];
    holder.SetTransaction(configs[0], txn);
    return holder;
  }

  TxnReplicaId GetTxnReplicaId(TransactionHolder& holder) {
    return holder.GetTransaction()->internal().replica_id();
  }
};

TEST_F(SimpleRemasterManagerTest, ValidateMetadata) {
  storage->Write("A", Record("value", 0, 1));
  storage->Write("B", Record("value", 0, 1));
  auto& txn1 = MakeHolder(MakeTransaction({"A", "B"}, {}, "some code", {{"B", {0, 1}}}), 100);
  auto& txn2 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {1, 1}}}), 101);
  ASSERT_ANY_THROW(remaster_manager->VerifyMaster(GetTxnReplicaId(txn1)));
  ASSERT_DEATH(remaster_manager->VerifyMaster(GetTxnReplicaId(txn2)), "Masters don't match");
}

TEST_F(SimpleRemasterManagerTest, CheckCounters) {
  storage->Write("A", Record("value", 0, 1));
  auto& txn1 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 1}}}), 100);
  auto& txn2 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 0}}}), 101);
  auto& txn3 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 2}}}), 102);

  ASSERT_EQ(remaster_manager->VerifyMaster(GetTxnReplicaId(txn1)), VerifyMasterResult::VALID);
  ASSERT_EQ(remaster_manager->VerifyMaster(GetTxnReplicaId(txn2)), VerifyMasterResult::ABORT);
  ASSERT_EQ(remaster_manager->VerifyMaster(GetTxnReplicaId(txn3)), VerifyMasterResult::WAITING);
}

TEST_F(SimpleRemasterManagerTest, CheckMultipleCounters) {
  storage->Write("A", Record("value", 0, 1));
  storage->Write("B", Record("value", 0, 1));
  auto& txn1 = MakeHolder(MakeTransaction({"A"}, {"B"}, "some code", {{"A", {0, 1}}, {"B", {0, 1}}}), 100);
  auto& txn2 = MakeHolder(MakeTransaction({"A", "B"}, {}, "some code", {{"A", {0, 0}}, {"B", {0, 1}}}), 101);
  auto& txn3 = MakeHolder(MakeTransaction({}, {"A", "B"}, "some code", {{"A", {0, 1}}, {"B", {0, 2}}}), 102);

  ASSERT_EQ(remaster_manager->VerifyMaster(GetTxnReplicaId(txn1)), VerifyMasterResult::VALID);
  ASSERT_EQ(remaster_manager->VerifyMaster(GetTxnReplicaId(txn2)), VerifyMasterResult::ABORT);
  ASSERT_EQ(remaster_manager->VerifyMaster(GetTxnReplicaId(txn3)), VerifyMasterResult::WAITING);
}

TEST_F(SimpleRemasterManagerTest, BlockLocalLog) {
  storage->Write("A", Record("value", 0, 1));
  storage->Write("B", Record("value", 1, 1));
  auto& txn1 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 2}}}), 100);
  auto& txn2 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 1}}}), 101);
  auto& txn3 = MakeHolder(MakeTransaction({"B"}, {}, "some code", {{"B", {1, 1}}}), 102);

  ASSERT_EQ(remaster_manager->VerifyMaster(GetTxnReplicaId(txn1)), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(GetTxnReplicaId(txn2)), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(GetTxnReplicaId(txn3)), VerifyMasterResult::VALID);
}

TEST_F(SimpleRemasterManagerTest, RemasterReleases) {
  storage->Write("A", Record("value", 0, 1));
  auto& txn1 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 2}}}), 100);
  auto& txn2 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 1}}}), 101);

  ASSERT_EQ(remaster_manager->VerifyMaster(GetTxnReplicaId(txn1)), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(GetTxnReplicaId(txn2)), VerifyMasterResult::WAITING);

  storage->Write("A", Record("value", 0, 2));
  auto result = remaster_manager->RemasterOccured("A", 2);
  ASSERT_THAT(result.unblocked, ElementsAre(100));
  ASSERT_THAT(result.should_abort, ElementsAre(101));
}
