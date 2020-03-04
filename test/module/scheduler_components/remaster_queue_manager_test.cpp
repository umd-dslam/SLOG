#include <gmock/gmock.h>

#include "common/test_utils.h"
#include "common/proto_utils.h"
#include "module/scheduler_components/remaster_queue_manager.h"

using namespace std;
using namespace slog;
using ::testing::ElementsAre;
using std::make_pair;

class RemasterQueueManagerTest : public ::testing::Test {
protected:
  void SetUp() {
    configs = MakeTestConfigurations("locking", 1, 1);
    storage = make_shared<slog::MemOnlyStorage<Key, Record, Metadata>>();
    all_txns = make_shared<unordered_map<TxnId, TransactionHolder>>();
    remaster_manager = make_unique<RemasterQueueManager>(configs[0], storage, all_txns);
  }

  ConfigVec configs;
  shared_ptr<Storage<Key, Record>> storage;
  shared_ptr<unordered_map<TxnId, TransactionHolder>> all_txns;
  unique_ptr<RemasterQueueManager> remaster_manager;

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

TEST_F(RemasterQueueManagerTest, CheckCounters) {
  storage->Write("A", Record("valueA", 0, 1));
  auto& txn1 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 1}}}), 100);
  auto& txn2 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 2}}}), 101);
  auto& txn3 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 0}}}), 102);

  ASSERT_EQ(remaster_manager->VerifyMaster(txn1), VerifyMasterResult::VALID);
  ASSERT_EQ(remaster_manager->VerifyMaster(txn2), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(txn3), VerifyMasterResult::ABORT);
}

TEST_F(RemasterQueueManagerTest, CheckMultipleCounters) {
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

TEST_F(RemasterQueueManagerTest, CheckIndirectBlocking) {
  storage->Write("A", Record("valueA", 0, 1));
  storage->Write("B", Record("valueB", 0, 1));
  auto& txn1 = MakeHolder(MakeTransaction({"A"}, {"B"}, "some code", {{"A", {0, 1}}, {"B", {0, 2}}}), 100);
  auto& txn2 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 1}}}), 101);
  auto& txn3 = MakeHolder(MakeTransaction({"B"}, {}, "some code", {{"B", {0, 1}}}), 102);

  ASSERT_EQ(remaster_manager->VerifyMaster(txn1), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(txn2), VerifyMasterResult::WAITING);
  ASSERT_EQ(remaster_manager->VerifyMaster(txn3), VerifyMasterResult::VALID);
}

// TEST_F(RemasterQueueManagerTest, RemasterQueueSingleKey) {
//   storage->Write("A", Record("valueA", 0, 1));
//   auto txn1 = MakeTransaction({"A"}, {}, "some code", {{"A", {0, 2}}});
//   txn1.mutable_internal()->set_id(100);

//   ASSERT_EQ(remaster_manager->VerifyMaster(txn1), VerifyMasterResult::WAITING);
//   auto unblocked = remaster_manager->RemasterOccured("A", 2);
//   ASSERT_THAT(unblocked, ElementsAre(100));
// }

// TEST_F(RemasterQueueManagerTest, RemasterQueueMultipleKeys) {
//   storage->Write("A", Record("valueA", 0, 1));
//   storage->Write("B", Record("valueB", 0, 1));
//   auto txn1 = MakeTransaction({"A"}, {"B"}, "some code", {{"A", {0, 2}}, {"B", {0, 3}}});
//   txn1.mutable_internal()->set_id(100);

//   ASSERT_EQ(remaster_manager->VerifyMaster(txn1), VerifyMasterResult::WAITING);
//   ASSERT_EQ(remaster_manager->RemasterOccured("B", 2).size(), 0);
//   ASSERT_EQ(remaster_manager->RemasterOccured("A", 2).size(), 0);
//   auto unblocked = remaster_manager->RemasterOccured("B", 3);
//   ASSERT_THAT(unblocked, ElementsAre(100));
// }

// TEST_F(RemasterQueueManagerTest, RemasterQueueMultipleTxns) {
//   storage->Write("A", Record("valueA", 0, 1));
//   storage->Write("B", Record("valueB", 0, 1));
//   auto txn1 = MakeTransaction({"A"}, {"B"}, "some code", {{"A", {0, 2}}, {"B", {0, 1}}});
//   txn1.mutable_internal()->set_id(100);
//   auto txn2 = MakeTransaction({"A"}, {"B"}, "some code", {{"A", {0, 2}}, {"B", {0, 2}}});
//   txn2.mutable_internal()->set_id(101);

//   ASSERT_EQ(remaster_manager->VerifyMaster(txn1), VerifyMasterResult::WAITING);
//   ASSERT_EQ(remaster_manager->VerifyMaster(txn2), VerifyMasterResult::WAITING);
//   ASSERT_EQ(remaster_manager->RemasterOccured("B", 2).size(), 0);
//   auto unblocked = remaster_manager->RemasterOccured("A", 2);
//   ASSERT_THAT(unblocked, ElementsAre(100, 101));
// }