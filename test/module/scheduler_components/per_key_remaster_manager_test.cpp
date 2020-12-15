#include <gmock/gmock.h>

#include "test/test_utils.h"
#include "common/proto_utils.h"
#include "module/scheduler_components/per_key_remaster_manager.h"

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
    remaster_manager = make_unique<PerKeyRemasterManager>(storage);
  }

  ConfigVec configs;
  shared_ptr<Storage<Key, Record>> storage;
  unique_ptr<RemasterManager> remaster_manager;
  MessagePool<internal::Request> pool;

  TransactionHolder MakeHolder(Transaction* txn, uint32_t txn_id = 0) {
    txn->mutable_internal()->set_id(txn_id);
    ReusableRequest req(&pool);
    req.get()->mutable_forward_txn()->set_allocated_txn(txn);
    return TransactionHolder{configs[0], move(req)};
  }
};

TEST_F(RemasterManagerTest, CheckCounters) {
  storage->Write("A", Record("valueA", 0, 1));
  auto txn1 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 1}}}), 100);
  auto txn2 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 2}}}), 101);
  auto txn3 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 0}}}), 102);

  ASSERT_EQ(remaster_manager->VerifyMaster(&txn1), VerifyMasterResult::VALID);
  ASSERT_EQ(remaster_manager->VerifyMaster(&txn2), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(&txn3), VerifyMasterResult::ABORT);
}

TEST_F(RemasterManagerTest, CheckMultipleCounters) {
  storage->Write("A", Record("valueA", 0, 1));
  storage->Write("B", Record("valueB", 0, 1));
  storage->Write("C", Record("valueC", 0, 1));
  auto txn1 = MakeHolder(MakeTransaction({"A", "C"}, {"B"}, "some code", {{"A", {0, 1}}, {"B", {0, 1}}, {"C", {0, 1}}}), 100);
  auto txn2 = MakeHolder(MakeTransaction({"A", "C"}, {"B"}, "some code", {{"A", {0, 1}}, {"B", {0, 1}}, {"C", {0, 2}}}), 101);
  auto txn3 = MakeHolder(MakeTransaction({"A", "C"}, {"B"}, "some code", {{"A", {0, 1}}, {"B", {0, 0}}, {"C", {0, 2}}}), 102);

  ASSERT_EQ(remaster_manager->VerifyMaster(&txn1), VerifyMasterResult::VALID);
  ASSERT_EQ(remaster_manager->VerifyMaster(&txn2), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(&txn3), VerifyMasterResult::ABORT);
}

TEST_F(RemasterManagerTest, CheckIndirectBlocking) {
  storage->Write("A", Record("valueA", 0, 1));
  storage->Write("B", Record("valueB", 0, 1));
  auto txn1 = MakeHolder(MakeTransaction({"A"}, {"B"}, "some code", {{"A", {0, 1}}, {"B", {0, 2}}}), 100);
  auto txn2 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 1}}}), 101);
  auto txn3 = MakeHolder(MakeTransaction({"B"}, {}, "some code", {{"B", {0, 1}}}), 102);

  ASSERT_EQ(remaster_manager->VerifyMaster(&txn1), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(&txn2), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(&txn3), VerifyMasterResult::VALID);
}

TEST_F(RemasterManagerTest, RemasterQueueSingleKey) {
  storage->Write("A", Record("valueA", 0, 1));
  auto txn1 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {1, 2}}}), 100);

  ASSERT_EQ(remaster_manager->VerifyMaster(&txn1), VerifyMasterResult::WAITING);
  storage->Write("A", Record("valueA", 1, 2));
  auto unblocked = remaster_manager->RemasterOccured("A", 2).unblocked;
  ASSERT_THAT(unblocked, ElementsAre(&txn1));
}

TEST_F(RemasterManagerTest, RemasterQueueMultipleKeys) {
  storage->Write("A", Record("valueA", 0, 1));
  storage->Write("B", Record("valueB", 0, 1));
  auto txn1 = MakeHolder(MakeTransaction({"A"}, {"B"}, "some code", {{"A", {1, 2}}, {"B", {0, 3}}}), 100);

  ASSERT_EQ(remaster_manager->VerifyMaster(&txn1), VerifyMasterResult::WAITING);
  storage->Write("B", Record("valueB", 1, 2));
  ASSERT_THAT(remaster_manager->RemasterOccured("B", 2).unblocked, ElementsAre());
  storage->Write("A", Record("valueA", 1, 2));
  ASSERT_THAT(remaster_manager->RemasterOccured("A", 2).unblocked, ElementsAre());
  storage->Write("B", Record("valueB", 0, 3));
  ASSERT_THAT(remaster_manager->RemasterOccured("B", 3).unblocked, ElementsAre(&txn1));
}

TEST_F(RemasterManagerTest, RemasterQueueMultipleTxns) {
  storage->Write("A", Record("valueA", 0, 1));
  storage->Write("B", Record("valueB", 0, 1));
  auto txn1 = MakeHolder(MakeTransaction({"A"}, {"B"}, "some code", {{"A", {1, 2}}, {"B", {1, 2}}}), 100);
  auto txn2 = MakeHolder(MakeTransaction({}, {"B"}, "some code", {{"B", {1, 2}}}), 101);

  ASSERT_EQ(remaster_manager->VerifyMaster(&txn1), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(&txn2), VerifyMasterResult::WAITING);
  storage->Write("B", Record("valueB", 1, 2));
  ASSERT_THAT(remaster_manager->RemasterOccured("B", 2).unblocked, ElementsAre());
  storage->Write("A", Record("valueA", 1, 2));
  ASSERT_THAT(remaster_manager->RemasterOccured("A", 2).unblocked, ElementsAre(&txn1, &txn2));
} 