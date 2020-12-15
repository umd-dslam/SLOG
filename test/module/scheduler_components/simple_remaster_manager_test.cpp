#include <gmock/gmock.h>

#include "test/test_utils.h"
#include "common/proto_utils.h"

#include "module/scheduler_components/simple_remaster_manager.h"

using namespace std;
using namespace slog;
using ::testing::ElementsAre;

class SimpleRemasterManagerTest : public ::testing::Test {
protected:
  void SetUp() {
    configs = MakeTestConfigurations("remaster", 1, 1);
    storage = make_shared<slog::MemOnlyStorage<Key, Record, Metadata>>();
    remaster_manager = make_unique<SimpleRemasterManager>(storage);
  }

  ConfigVec configs;
  shared_ptr<Storage<Key, Record>> storage;
  unique_ptr<SimpleRemasterManager> remaster_manager;
  MessagePool<internal::Request> pool;

  TransactionHolder MakeHolder(Transaction* txn, uint32_t txn_id = 0) {
    txn->mutable_internal()->set_id(txn_id);
    ReusableRequest req(&pool);
    req.get()->mutable_forward_txn()->set_allocated_txn(txn);
    return TransactionHolder{configs[0], move(req)};
  }
};

TEST_F(SimpleRemasterManagerTest, ValidateMetadata) {
  storage->Write("A", Record("value", 0, 1));
  storage->Write("B", Record("value", 0, 1));
  auto t = MakeTransaction({"A", "B"}, {}, "some code", {{"B", {0, 1}}});
  auto txn1 = MakeHolder(t);
  auto t2 = MakeTransaction({"A"}, {}, "some code", {{"A", {1, 1}}});
  auto txn2 = MakeHolder(t2);
  ASSERT_ANY_THROW(remaster_manager->VerifyMaster(&txn1));
  ASSERT_DEATH(remaster_manager->VerifyMaster(&txn2), "Masters don't match");
}

TEST_F(SimpleRemasterManagerTest, CheckCounters) {
  storage->Write("A", Record("value", 0, 1));
  auto txn1 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 1}}}));
  auto txn2 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 0}}}));
  auto txn3 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 2}}}));

  ASSERT_EQ(remaster_manager->VerifyMaster(&txn1), VerifyMasterResult::VALID);
  ASSERT_EQ(remaster_manager->VerifyMaster(&txn2), VerifyMasterResult::ABORT);
  ASSERT_EQ(remaster_manager->VerifyMaster(&txn3), VerifyMasterResult::WAITING);
}

TEST_F(SimpleRemasterManagerTest, CheckMultipleCounters) {
  storage->Write("A", Record("value", 0, 1));
  storage->Write("B", Record("value", 0, 1));
  auto txn1 = MakeHolder(MakeTransaction({"A"}, {"B"}, "some code", {{"A", {0, 1}}, {"B", {0, 1}}}));
  auto txn2 = MakeHolder(MakeTransaction({"A", "B"}, {}, "some code", {{"A", {0, 0}}, {"B", {0, 1}}}));
  auto txn3 = MakeHolder(MakeTransaction({}, {"A", "B"}, "some code", {{"A", {0, 1}}, {"B", {0, 2}}}));

  ASSERT_EQ(remaster_manager->VerifyMaster(&txn1), VerifyMasterResult::VALID);
  ASSERT_EQ(remaster_manager->VerifyMaster(&txn2), VerifyMasterResult::ABORT);
  ASSERT_EQ(remaster_manager->VerifyMaster(&txn3), VerifyMasterResult::WAITING);
}

TEST_F(SimpleRemasterManagerTest, BlockLocalLog) {
  storage->Write("A", Record("value", 0, 1));
  storage->Write("B", Record("value", 1, 1));
  auto txn1 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 2}}}));
  auto txn2 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 1}}}));
  auto txn3 = MakeHolder(MakeTransaction({"B"}, {}, "some code", {{"B", {1, 1}}}));
  ASSERT_EQ(remaster_manager->VerifyMaster(&txn1), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(&txn2), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(&txn3), VerifyMasterResult::VALID);
}

TEST_F(SimpleRemasterManagerTest, RemasterUnblocks) {
  storage->Write("A", Record("value", 0, 1));
  auto txn1 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 2}}}));
  auto txn2 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 1}}}));

  ASSERT_EQ(remaster_manager->VerifyMaster(&txn1), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(&txn2), VerifyMasterResult::WAITING);

  storage->Write("A", Record("value", 0, 2));
  auto result = remaster_manager->RemasterOccured("A", 2);
  ASSERT_THAT(result.unblocked, ElementsAre(&txn1));
  ASSERT_THAT(result.should_abort, ElementsAre(&txn2));
}

TEST_F(SimpleRemasterManagerTest, ReleaseTransaction) {
  storage->Write("A", Record("value", 0, 1));
  storage->Write("B", Record("valueB", 1, 1));
  auto txn1 = MakeHolder(MakeTransaction({"B"}, {}, "some code", {{"B", {0, 2}}}), 100);
  auto txn2 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 1}}}), 101);
  auto txn3 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 1}}}), 102);

  ASSERT_EQ(remaster_manager->VerifyMaster(&txn1), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(&txn2), VerifyMasterResult::WAITING);

  auto result = remaster_manager->ReleaseTransaction(&txn3);
  ASSERT_THAT(result.unblocked, ElementsAre());
  ASSERT_THAT(result.should_abort, ElementsAre());

  result = remaster_manager->ReleaseTransaction(&txn1);
  ASSERT_THAT(result.unblocked, ElementsAre(&txn2));
  ASSERT_THAT(result.should_abort, ElementsAre());
}
