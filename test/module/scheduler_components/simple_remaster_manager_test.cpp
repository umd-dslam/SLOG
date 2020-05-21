#include <gmock/gmock.h>
#include <fstream>
#include "common/test_utils.h"
#include "common/proto_utils.h"

#include "module/scheduler_components/simple_remaster_manager.h"
#include "module/scheduler_components/per_key_remaster_manager.h"

using namespace std;
using namespace slog;
using ::testing::ElementsAre;

class SimpleRemasterManagerTest : public ::testing::Test {
protected:
  void SetUp() {
    configs = MakeTestConfigurations("remaster", 1, 1);
    storage = make_shared<slog::MemOnlyStorage<Key, Record, Metadata>>();
    remaster_manager = make_unique<PerKeyRemasterManager>(storage);
  }

  ConfigVec configs;
  shared_ptr<Storage<Key, Record>> storage;
  unique_ptr<PerKeyRemasterManager> remaster_manager;

  TransactionHolder MakeHolder(Transaction* txn, uint32_t txn_id = 0) {
    txn->mutable_internal()->set_id(txn_id);
    return TransactionHolder(configs[0], txn);
  }

  void RunTest(RemasterManager* remaster_manager) {
  auto start_time = std::chrono::steady_clock::now();

  auto alpha_size = 1000;
  auto txn_size = 300;
  std::unordered_map<string, uint32_t> rep0;
  std::unordered_map<string, uint32_t> rep1;
  std::unordered_map<TxnId, Transaction> txns;
  std::unordered_map<TxnId, TransactionHolder> txn_holders;

  std::unordered_map<TxnId, TimePoint> txn_starts;
  std::unordered_map<TxnId, TimePoint> txn_stops;
  auto aborts = 0;

  for(int32_t i = 0; i < alpha_size; i++) {
    string s = std::to_string(i);
    storage->Write(s, Record("value", i % 2, 0));
    if (i % 2 == 0) {
      rep0.insert({s, 0});
    } else {
      rep1.insert({s, 0});
    }
  }

  for (int32_t i = 0; i < 1000; i++) {

    // LOG(INFO) << "rep0 " << rep0.size();
    // LOG(INFO) << "rep1 " << rep1.size();

    auto old_rep = 0;
    string mov_s = "";
    auto old_counter = 0;

    if (rep0.size() != 1 && rand() % 2 == 0 || rep1.size() == 1) {
      auto r = std::next(std::begin(rep0), rand() % rep0.size());
      mov_s = r->first;
      old_rep = 0;
      old_counter = r->second;
    } else {
      auto r = std::next(std::begin(rep1), rand() % rep1.size());
      mov_s = r->first;
      old_rep = 1;
      old_counter = r->second;
    }

    // submit waiting txn
    auto txn_id = i * 10;
    ASSERT_TRUE(txns.count(txn_id) == 0);
    usleep(10000);
    Transaction& txn = txns[txn_id];
    txn.mutable_internal()->set_id(txn_id);
    txn_starts.insert({txn_id, std::chrono::steady_clock::now()});
    if (old_rep == 0) {
      for(int32_t j = 0; j < txn_size; j++) {
        auto random_it = std::next(std::begin(rep1), rand() % rep1.size());
        MasterMetadata metadata;
        metadata.set_master(1);
        metadata.set_counter(random_it->second);
        txn.mutable_read_set()->insert({random_it->first, ""});
        txn.mutable_internal()->mutable_master_metadata()->insert({random_it->first, std::move(metadata)});
      }
      MasterMetadata metadata;
      metadata.set_master(1);
      metadata.set_counter(old_counter+1);
      txn.mutable_read_set()->insert({mov_s, ""});
      txn.mutable_internal()->mutable_master_metadata()->insert({mov_s, std::move(metadata)});
    } else {
      for(int32_t j = 0; j < txn_size; j++) {
        auto random_it = std::next(std::begin(rep0), rand() % rep0.size());
        MasterMetadata metadata;
        metadata.set_master(0);
        metadata.set_counter(random_it->second);
        txn.mutable_read_set()->insert({random_it->first, ""});
        txn.mutable_internal()->mutable_master_metadata()->insert({random_it->first, std::move(metadata)});
      }
      MasterMetadata metadata;
      metadata.set_master(0);
      metadata.set_counter(old_counter+1);
      txn.mutable_read_set()->insert({mov_s, ""});
      txn.mutable_internal()->mutable_master_metadata()->insert({mov_s, std::move(metadata)});
    }
    auto& txn2 = txn_holders[txn_id];
    txn2.SetTransaction(configs[0], &txn);
    ASSERT_EQ(remaster_manager->VerifyMaster(&txn2), VerifyMasterResult::WAITING);

    // submit normal txns
    for (auto x = 1; x < 10; x++) {
      auto txn_id = i * 10 + x;
      ASSERT_TRUE(txns.count(txn_id) == 0);
      usleep(10000);
      Transaction& txn = txns[txn_id];
      txn.mutable_internal()->set_id(txn_id);
      txn_starts.insert({txn_id, std::chrono::steady_clock::now()});
      for(int32_t j = 0; j < txn_size; j++) {
        if (i % 2 == 0 || rep1.size() == 0) {
          auto random_it = std::next(std::begin(rep0), rand() % rep0.size());
          MasterMetadata metadata;
          metadata.set_master(0);
          metadata.set_counter(random_it->second);
          txn.mutable_read_set()->insert({random_it->first, ""});
          txn.mutable_internal()->mutable_master_metadata()->insert({random_it->first, std::move(metadata)});
        }
        else {
          auto random_it = std::next(std::begin(rep1), rand() % rep1.size());
          MasterMetadata metadata;
          metadata.set_master(1);
          metadata.set_counter(random_it->second);
          txn.mutable_read_set()->insert({random_it->first, ""});
          txn.mutable_internal()->mutable_master_metadata()->insert({random_it->first, std::move(metadata)});
        }
      }
      auto& txn1 = txn_holders[txn_id];
      txn1.SetTransaction(configs[0], &txn);
      auto result = remaster_manager->VerifyMaster(&txn1);
      if (result == VerifyMasterResult::VALID) {
        txn_stops.insert({txn_id, std::chrono::steady_clock::now()});
      } else if (result == VerifyMasterResult::ABORT) {
        txn_stops.insert({txn_id, std::chrono::steady_clock::now()});
        aborts += 1;
      }
    }

    // Do remaster
    RemasterOccurredResult result;
    if (old_rep == 0) {
      rep1.insert({mov_s, old_counter + 1});
      storage->Write(mov_s, Record("value", 1, old_counter + 1));
      rep0.erase(mov_s);
      result = remaster_manager->RemasterOccured(mov_s, old_counter + 1);
    } else {
      rep0.insert({mov_s, old_counter + 1});
      storage->Write(mov_s, Record("value", 0, old_counter + 1));
      rep1.erase(mov_s);
      result = remaster_manager->RemasterOccured(mov_s, old_counter + 1);
    }

    for (auto txn : result.unblocked) {
      txn_stops.insert({txn->GetTransaction()->internal().id(), std::chrono::steady_clock::now()});
    }
    for (auto txn : result.should_abort) {
      txn_stops.insert({txn->GetTransaction()->internal().id(), std::chrono::steady_clock::now()});
      aborts += 1;
    }
  }

  auto duration = (std::chrono::steady_clock::now() - start_time).count();
  LOG(INFO) << "duration " << duration;
  LOG(INFO) << "started " << txn_starts.size();
  LOG(INFO) << "stopped " << txn_stops.size();
  LOG(INFO) << "aborts " << aborts;

  std::ofstream myfile ("silly_latency.txt" + std::to_string(duration));
  for (auto t : txn_stops) {
    auto latency = (t.second - txn_starts[t.first]).count();
    myfile << std::to_string(latency) << "\n";
  }
  myfile.close();

  for (auto& t : txn_holders) {
    t.second.ReleaseTransaction();
  }
}
};

// TEST_F(SimpleRemasterManagerTest, ValidateMetadata) {
//   storage->Write("A", Record("value", 0, 1));
//   storage->Write("B", Record("value", 0, 1));
//   auto t = MakeTransaction({"A", "B"}, {}, "some code", {{"B", {0, 1}}});
//   auto txn1 = MakeHolder(t);
//   auto t2 = MakeTransaction({"A"}, {}, "some code", {{"A", {1, 1}}});
//   auto txn2 = MakeHolder(t2);
//   ASSERT_ANY_THROW(remaster_manager->VerifyMaster(&txn1));
//   ASSERT_DEATH(remaster_manager->VerifyMaster(&txn2), "Masters don't match");
// }

// TEST_F(SimpleRemasterManagerTest, CheckCounters) {
//   storage->Write("A", Record("value", 0, 1));
//   auto txn1 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 1}}}));
//   auto txn2 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 0}}}));
//   auto txn3 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 2}}}));

//   ASSERT_EQ(remaster_manager->VerifyMaster(&txn1), VerifyMasterResult::VALID);
//   ASSERT_EQ(remaster_manager->VerifyMaster(&txn2), VerifyMasterResult::ABORT);
//   ASSERT_EQ(remaster_manager->VerifyMaster(&txn3), VerifyMasterResult::WAITING);
// }

// TEST_F(SimpleRemasterManagerTest, CheckMultipleCounters) {
//   storage->Write("A", Record("value", 0, 1));
//   storage->Write("B", Record("value", 0, 1));
//   auto txn1 = MakeHolder(MakeTransaction({"A"}, {"B"}, "some code", {{"A", {0, 1}}, {"B", {0, 1}}}));
//   auto txn2 = MakeHolder(MakeTransaction({"A", "B"}, {}, "some code", {{"A", {0, 0}}, {"B", {0, 1}}}));
//   auto txn3 = MakeHolder(MakeTransaction({}, {"A", "B"}, "some code", {{"A", {0, 1}}, {"B", {0, 2}}}));

//   ASSERT_EQ(remaster_manager->VerifyMaster(&txn1), VerifyMasterResult::VALID);
//   ASSERT_EQ(remaster_manager->VerifyMaster(&txn2), VerifyMasterResult::ABORT);
//   ASSERT_EQ(remaster_manager->VerifyMaster(&txn3), VerifyMasterResult::WAITING);
// }

// TEST_F(SimpleRemasterManagerTest, BlockLocalLog) {
//   storage->Write("A", Record("value", 0, 1));
//   storage->Write("B", Record("value", 1, 1));
//   auto txn1 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 2}}}));
//   auto txn2 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 1}}}));
//   auto txn3 = MakeHolder(MakeTransaction({"B"}, {}, "some code", {{"B", {1, 1}}}));
//   ASSERT_EQ(remaster_manager->VerifyMaster(&txn1), VerifyMasterResult::WAITING);
//   ASSERT_EQ(remaster_manager->VerifyMaster(&txn2), VerifyMasterResult::WAITING);
//   ASSERT_EQ(remaster_manager->VerifyMaster(&txn3), VerifyMasterResult::VALID);
// }

// TEST_F(SimpleRemasterManagerTest, RemasterReleases) {
//   storage->Write("A", Record("value", 0, 1));
//   auto txn1 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 2}}}));
//   auto txn2 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 1}}}));

//   ASSERT_EQ(remaster_manager->VerifyMaster(&txn1), VerifyMasterResult::WAITING);
//   ASSERT_EQ(remaster_manager->VerifyMaster(&txn2), VerifyMasterResult::WAITING);

//   storage->Write("A", Record("value", 0, 2));
//   auto result = remaster_manager->RemasterOccured("A", 2);
//   ASSERT_THAT(result.unblocked, ElementsAre(&txn1));
//   ASSERT_THAT(result.should_abort, ElementsAre(&txn2));
// }

// TEST_F(SimpleRemasterManagerTest, ReleaseTransaction) {
//   storage->Write("A", Record("value", 0, 1));
//   auto txn1 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 2}}}), 100);
//   auto txn2 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 1}}}), 101);

//   ASSERT_EQ(remaster_manager->VerifyMaster(&txn1), VerifyMasterResult::WAITING);
//   ASSERT_EQ(remaster_manager->VerifyMaster(&txn2), VerifyMasterResult::WAITING);

//   auto result = remaster_manager->ReleaseTransaction(100);
//   ASSERT_THAT(result.unblocked, ElementsAre(&txn2));
//   ASSERT_THAT(result.should_abort, ElementsAre());
// }

// TEST_F(SimpleRemasterManagerTest, ReleaseTransactionInPartition) {
//   storage->Write("A", Record("value", 0, 1));
//   storage->Write("B", Record("value", 1, 1));
//   auto txn1 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 2}}}), 100);
//   auto txn2 = MakeHolder(MakeTransaction({"B"}, {}, "some code", {{"B", {1, 2}}}), 101);
//   auto txn3 = MakeHolder(MakeTransaction({"A"}, {}, "some code", {{"A", {0, 1}}}), 102);

//   EXPECT_EQ(remaster_manager->VerifyMaster(&txn1), VerifyMasterResult::WAITING);
//   ASSERT_EQ(remaster_manager->VerifyMaster(&txn2), VerifyMasterResult::WAITING);
//   ASSERT_EQ(remaster_manager->VerifyMaster(&txn3), VerifyMasterResult::WAITING);

//   unordered_set<uint32_t> partition_1({1});
//   auto result = remaster_manager->ReleaseTransaction(100, partition_1);
//   ASSERT_THAT(result.unblocked, ElementsAre());
//   ASSERT_THAT(result.should_abort, ElementsAre());

//   unordered_set<uint32_t> partition_0({0});
//   result = remaster_manager->ReleaseTransaction(100, partition_0);
//   ASSERT_THAT(result.unblocked, ElementsAre(&txn3));
//   ASSERT_THAT(result.should_abort, ElementsAre());
// }

TEST_F(SimpleRemasterManagerTest, Simple) {
  auto rm = SimpleRemasterManager(storage);
  RunTest(&rm);
}

TEST_F(SimpleRemasterManagerTest, PerKey) {
  auto rm = PerKeyRemasterManager(storage);
  RunTest(&rm);
}

