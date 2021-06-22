#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/proto_utils.h"
#include "connection/broker.h"
#include "module/forwarder.h"
#include "module/server.h"
#include "proto/api.pb.h"
#include "storage/mem_only_storage.h"
#include "test/test_utils.h"

using namespace std;
using namespace slog;

class E2ETest : public ::testing::Test {
 protected:
  static const size_t NUM_MACHINES = 4;

  virtual internal::Configuration CustomConfig() { return internal::Configuration(); }

  void SetUp() {
    auto custom_config = CustomConfig();
    custom_config.set_replication_factor(2);
    custom_config.add_replication_order("1");
    custom_config.add_replication_order("0");
    configs = MakeTestConfigurations("e2e", 2 /* num_replicas */, 2 /* num_partitions */, custom_config);

    for (size_t i = 0; i < NUM_MACHINES; i++) {
      test_slogs[i] = make_unique<TestSlog>(configs[i]);

      test_slogs[i]->AddServerAndClient();
      test_slogs[i]->AddForwarder();
      test_slogs[i]->AddMultiHomeOrderer();
      test_slogs[i]->AddSequencer();
      test_slogs[i]->AddInterleaver();
      test_slogs[i]->AddScheduler();
      test_slogs[i]->AddLocalPaxos();

      // One region is selected to globally order the multihome batches
      if (configs[i]->leader_replica_for_multi_home_ordering() == configs[i]->local_replica()) {
        test_slogs[i]->AddGlobalPaxos();
      }
    }

    // Replica 0
    test_slogs[0]->Data("A", {"valA", 0, 0});
    test_slogs[0]->Data("C", {"valC", 1, 1});
    test_slogs[1]->Data("B", {"valB", 0, 1});
    test_slogs[1]->Data("X", {"valX", 1, 0});
    // Replica 1
    test_slogs[2]->Data("A", {"valA", 0, 0});
    test_slogs[2]->Data("C", {"valC", 1, 1});
    test_slogs[3]->Data("B", {"valB", 0, 1});
    test_slogs[3]->Data("X", {"valX", 1, 0});

    for (const auto& test_slog : test_slogs) {
      test_slog->StartInNewThreads();
    }
  }

  unique_ptr<TestSlog> test_slogs[NUM_MACHINES];
  ConfigVec configs;
};

// This test submits multiple transactions to the system serially and checks the
// read values for correctness.
TEST_F(E2ETest, BasicSingleHomeSingleParition) {
  auto txn1 = MakeTransaction({{"A", KeyType::WRITE}}, {{"SET", "A", "newA"}});
  auto txn2 = MakeTransaction({{"A", KeyType::READ}}, {{"GET", "A"}});

  test_slogs[0]->SendTxn(txn1);
  auto txn1_resp = test_slogs[0]->RecvTxnResult();
  ASSERT_EQ(txn1_resp.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(txn1_resp.internal().type(), TransactionType::SINGLE_HOME);
  ASSERT_EQ(txn1_resp.keys_size(), 1);
  ASSERT_EQ(TxnValueEntry(txn1_resp, "A").value(), "valA");
  ASSERT_EQ(TxnValueEntry(txn1_resp, "A").new_value(), "newA");

  test_slogs[0]->SendTxn(txn2);
  auto txn2_resp = test_slogs[0]->RecvTxnResult();
  ASSERT_EQ(txn2_resp.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(txn2_resp.internal().type(), TransactionType::SINGLE_HOME);
  ASSERT_EQ(txn2_resp.keys_size(), 1);
  ASSERT_EQ(TxnValueEntry(txn2_resp, "A").value(), "newA");
}

TEST_F(E2ETest, MultiPartitionTxn) {
  for (size_t i = 0; i < NUM_MACHINES; i++) {
    auto txn = MakeTransaction({{"A", KeyType::READ}, {"B", KeyType::WRITE}}, {{"GET", "A"}, {"SET", "B", "newB"}});

    test_slogs[i]->SendTxn(txn);
    auto txn_resp = test_slogs[i]->RecvTxnResult();
    ASSERT_EQ(txn_resp.status(), TransactionStatus::COMMITTED);
    ASSERT_EQ(txn_resp.internal().type(), TransactionType::SINGLE_HOME);
    ASSERT_EQ(txn_resp.keys().size(), 2);
    ASSERT_EQ(TxnValueEntry(txn_resp, "A").value(), "valA");
    ASSERT_EQ(TxnValueEntry(txn_resp, "B").new_value(), "newB");
  }
}

TEST_F(E2ETest, MultiHomeTxn) {
  for (size_t i = 0; i < NUM_MACHINES; i++) {
    auto txn = MakeTransaction({{"A", KeyType::READ}, {"C", KeyType::WRITE}}, {{"GET", "A"}, {"SET", "C", "newC"}});

    test_slogs[i]->SendTxn(txn);
    auto txn_resp = test_slogs[i]->RecvTxnResult();
    ASSERT_EQ(txn_resp.status(), TransactionStatus::COMMITTED);
    ASSERT_EQ(txn_resp.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
    ASSERT_EQ(txn_resp.keys().size(), 2);
    ASSERT_EQ(TxnValueEntry(txn_resp, "A").value(), "valA");
    ASSERT_EQ(TxnValueEntry(txn_resp, "C").new_value(), "newC");
  }
}

TEST_F(E2ETest, MultiHomeMultiPartitionTxn) {
  for (size_t i = 0; i < NUM_MACHINES; i++) {
    auto txn = MakeTransaction({{"A", KeyType::READ}, {"X", KeyType::READ}, {"C", KeyType::READ}});

    test_slogs[i]->SendTxn(txn);
    auto txn_resp = test_slogs[i]->RecvTxnResult();
    ASSERT_EQ(txn_resp.status(), TransactionStatus::COMMITTED);
    ASSERT_EQ(txn_resp.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
    ASSERT_EQ(txn_resp.keys().size(), 3);
    ASSERT_EQ(TxnValueEntry(txn_resp, "A").value(), "valA");
    ASSERT_EQ(TxnValueEntry(txn_resp, "X").value(), "valX");
    ASSERT_EQ(TxnValueEntry(txn_resp, "C").value(), "valC");
  }
}

#ifdef ENABLE_REMASTER
TEST_F(E2ETest, RemasterTxn) {
  auto remaster_txn = MakeTransaction({{"A", KeyType::WRITE}}, {}, 1);

  test_slogs[1]->SendTxn(remaster_txn);
  auto remaster_txn_resp = test_slogs[1]->RecvTxnResult();
  ASSERT_EQ(TransactionStatus::COMMITTED, remaster_txn_resp.status());

#ifdef REMASTER_PROTOCOL_COUNTERLESS
  ASSERT_EQ(TransactionType::MULTI_HOME_OR_LOCK_ONLY, remaster_txn_resp.internal().type());
#else
  ASSERT_EQ(TransactionType::SINGLE_HOME, remaster_txn_resp.internal().type());
#endif /* REMASTER_PROTOCOL_COUNTERLESS */

  auto txn = MakeTransaction({{"A"}, {"X"}});

  // Since replication factor is set to 2 for all tests in this file, it is
  // guaranteed that this txn will see the changes made by the remaster txn
  test_slogs[1]->SendTxn(txn);
  auto txn_resp = test_slogs[1]->RecvTxnResult();
  ASSERT_EQ(txn_resp.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(txn_resp.internal().type(), TransactionType::SINGLE_HOME);  // used to be MH
  ASSERT_EQ(txn_resp.keys().size(), 2);
  ASSERT_EQ(TxnValueEntry(txn_resp, "A").value(), "valA");
  ASSERT_EQ(TxnValueEntry(txn_resp, "X").value(), "valX");
}
#endif

TEST_F(E2ETest, AbortTxnBadCommand) {
  // Multi-partition transaction where one of the partition will abort
  auto aborted_txn = MakeTransaction({{"A"}, {"B", KeyType::WRITE}}, {{"SET", "B", "notB"}, {"EQ", "A", "notA"}});

  test_slogs[1]->SendTxn(aborted_txn);
  auto aborted_txn_resp = test_slogs[1]->RecvTxnResult();
  ASSERT_EQ(TransactionStatus::ABORTED, aborted_txn_resp.status());
  ASSERT_EQ(TransactionType::SINGLE_HOME, aborted_txn_resp.internal().type());

  auto txn = MakeTransaction({{"B"}}, {{"GET", "B"}});

  test_slogs[1]->SendTxn(txn);
  auto txn_resp = test_slogs[1]->RecvTxnResult();
  ASSERT_EQ(txn_resp.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(txn_resp.internal().type(), TransactionType::SINGLE_HOME);
  // Value of B must not change because the previous txn was aborted
  ASSERT_EQ(TxnValueEntry(txn_resp, "B").value(), "valB");
}

TEST_F(E2ETest, AbortTxnEmptyKeySets) {
  // Multi-partition transaction where one of the partition will abort
  auto aborted_txn = MakeTransaction({});

  test_slogs[1]->SendTxn(aborted_txn);
  auto aborted_txn_resp = test_slogs[1]->RecvTxnResult();
  ASSERT_EQ(TransactionStatus::ABORTED, aborted_txn_resp.status());
  ASSERT_EQ(TransactionType::UNKNOWN, aborted_txn_resp.internal().type());
}

class E2ETestBypassMHOrderer : public E2ETest {
  internal::Configuration CustomConfig() final {
    internal::Configuration config;
    config.set_bypass_mh_orderer(true);
    return config;
  }
};

TEST_F(E2ETestBypassMHOrderer, MultiHomeSinglePartitionTxn) {
  for (size_t i = 0; i < NUM_MACHINES; i++) {
    auto txn = MakeTransaction({{"A"}, {"C"}});

    test_slogs[i]->SendTxn(txn);
    auto txn_resp = test_slogs[i]->RecvTxnResult();
    ASSERT_EQ(TransactionStatus::COMMITTED, txn_resp.status());
    ASSERT_EQ(TransactionType::MULTI_HOME_OR_LOCK_ONLY, txn_resp.internal().type());
    ASSERT_EQ("valA", TxnValueEntry(txn_resp, "A").value());
    ASSERT_EQ("valC", TxnValueEntry(txn_resp, "C").value());
  }
}

TEST_F(E2ETestBypassMHOrderer, MultiHomeMultiPartitionTxn) {
  for (size_t i = 0; i < NUM_MACHINES; i++) {
    auto txn = MakeTransaction({{"A", KeyType::READ}, {"X", KeyType::READ}, {"C", KeyType::READ}});

    test_slogs[i]->SendTxn(txn);
    auto txn_resp = test_slogs[i]->RecvTxnResult();
    ASSERT_EQ(txn_resp.status(), TransactionStatus::COMMITTED);
    ASSERT_EQ(txn_resp.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
    ASSERT_EQ(txn_resp.keys().size(), 3);
    ASSERT_EQ(TxnValueEntry(txn_resp, "A").value(), "valA");
    ASSERT_EQ(TxnValueEntry(txn_resp, "X").value(), "valX");
    ASSERT_EQ(TxnValueEntry(txn_resp, "C").value(), "valC");
  }
}

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  google::InstallFailureSignalHandler();
  return RUN_ALL_TESTS();
}