#include "module/forwarder.h"

#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/proto_utils.h"
#include "connection/broker.h"
#include "module/server.h"
#include "proto/api.pb.h"
#include "storage/mem_only_storage.h"
#include "test/test_utils.h"

using namespace std;
using namespace slog;

class ForwarderTest : public ::testing::Test {
 protected:
  static const size_t NUM_MACHINES = 4;

  void SetUp() {
    configs = MakeTestConfigurations("forwarder", 2 /* num_replicas */, 2 /* num_partitions */);

    for (size_t i = 0; i < NUM_MACHINES; i++) {
      test_slogs[i] = make_unique<TestSlog>(configs[i]);
      test_slogs[i]->AddServerAndClient();
      test_slogs[i]->AddForwarder();
      test_slogs[i]->AddOutputChannel(kSequencerChannel);
      test_slogs[i]->AddOutputChannel(kMultiHomeOrdererChannel);
    }
    // Replica 0
    test_slogs[0]->Data("A", {"xxxxx", 0, 0});
    test_slogs[0]->Data("C", {"xxxxx", 1, 1});
    test_slogs[1]->Data("B", {"xxxxx", 0, 1});
    test_slogs[1]->Data("X", {"xxxxx", 1, 0});
    // Replica 1
    test_slogs[2]->Data("A", {"xxxxx", 0, 0});
    test_slogs[2]->Data("C", {"xxxxx", 1, 1});
    test_slogs[3]->Data("B", {"xxxxx", 0, 1});
    test_slogs[3]->Data("X", {"xxxxx", 1, 0});

    for (const auto& test_slog : test_slogs) {
      test_slog->StartInNewThreads();
    }
  }

  Transaction* ReceiveOnSequencerChannel(vector<size_t> indices) {
    CHECK(!indices.empty());
    vector<zmq::pollitem_t> poll_items;
    for (auto i : indices) {
      poll_items.push_back(test_slogs[i]->GetPollItemForChannel(kSequencerChannel));
    }
    auto rc = zmq::poll(poll_items);
    if (rc <= 0) return nullptr;

    for (size_t i = 0; i < poll_items.size(); i++) {
      if (poll_items[i].revents & ZMQ_POLLIN) {
        auto req_env = test_slogs[indices[i]]->ReceiveFromOutputChannel(kSequencerChannel);
        if (req_env == nullptr) {
          return nullptr;
        }
        return ExtractTxn(req_env);
      }
    }

    return nullptr;
  }

  Transaction* ReceiveOnOrdererChannel(size_t index) {
    auto req_env = test_slogs[index]->ReceiveFromOutputChannel(kMultiHomeOrdererChannel);
    if (req_env == nullptr) {
      return nullptr;
    }
    return ExtractTxn(req_env);
  }

  unique_ptr<TestSlog> test_slogs[NUM_MACHINES];
  ConfigVec configs;

 private:
  Transaction* ExtractTxn(EnvelopePtr& req) {
    if (req->request().type_case() != internal::Request::kForwardTxn) {
      return nullptr;
    }
    return req->mutable_request()->mutable_forward_txn()->release_txn();
  }
};

TEST_F(ForwarderTest, ForwardToSameRegion) {
  // This txn needs to lookup from both partitions in a region
  auto txn = MakeTransaction({{"A"}, {"B", KeyType::WRITE}});
  // Send to partition 0 of replica 0
  test_slogs[0]->SendTxn(txn);
  auto forwarded_txn = ReceiveOnSequencerChannel({0});

  // The txn should be forwarded to the sequencer of the same machine
  ASSERT_TRUE(forwarded_txn != nullptr);
  ASSERT_EQ(TransactionType::SINGLE_HOME, forwarded_txn->internal().type());
  ASSERT_EQ(0, forwarded_txn->internal().home());
  ASSERT_EQ(0U, forwarded_txn->keys().at("A").metadata().master());
  ASSERT_EQ(0U, forwarded_txn->keys().at("A").metadata().counter());
  ASSERT_EQ(0U, forwarded_txn->keys().at("B").metadata().master());
  ASSERT_EQ(1U, forwarded_txn->keys().at("B").metadata().counter());
}

TEST_F(ForwarderTest, ForwardToAnotherRegion) {
  // Send to partition 1 of replica 0. This txn needs to lookup
  // from both partitions and later forwarded to replica 1
  test_slogs[1]->SendTxn(MakeTransaction({{"C"}, {"X", KeyType::WRITE}}));

  // Send to partition 0 of replica 1. This txn needs to lookup
  // from partition 0 only and later forwarded to replica 0
  test_slogs[2]->SendTxn(MakeTransaction({{"A"}}));

  {
    auto forwarded_txn = ReceiveOnSequencerChannel({2, 3});
    // A txn should be forwarded to one of the two schedulers in
    // replica 1
    ASSERT_TRUE(forwarded_txn != nullptr);
    ASSERT_EQ(TransactionType::SINGLE_HOME, forwarded_txn->internal().type());
    ASSERT_EQ(1, forwarded_txn->internal().home());
    ASSERT_EQ(1U, forwarded_txn->keys().at("C").metadata().master());
    ASSERT_EQ(1U, forwarded_txn->keys().at("C").metadata().counter());
    ASSERT_EQ(1U, forwarded_txn->keys().at("X").metadata().master());
    ASSERT_EQ(0U, forwarded_txn->keys().at("X").metadata().counter());
  }

  {
    auto forwarded_txn = ReceiveOnSequencerChannel({0, 1});
    // A txn should be forwarded to one of the two schedulers in
    // replica 0
    ASSERT_TRUE(forwarded_txn != nullptr);
    ASSERT_EQ(TransactionType::SINGLE_HOME, forwarded_txn->internal().type());
    ASSERT_EQ(0, forwarded_txn->internal().home());
    ASSERT_EQ(0U, forwarded_txn->keys().at("A").metadata().master());
    ASSERT_EQ(0U, forwarded_txn->keys().at("A").metadata().counter());
  }
}

TEST_F(ForwarderTest, TransactionHasNewKeys) {
  // This txn needs to lookup from both partitions in a region
  auto txn = MakeTransaction({{"NEW"}, {"KEY", KeyType::WRITE}});
  // Send to partition 0 of replica 0
  test_slogs[3]->SendTxn(txn);

  auto forwarded_txn = ReceiveOnSequencerChannel({0, 1, 2, 3});
  // The txn should be forwarded to the scheduler of the same machine
  ASSERT_TRUE(forwarded_txn != nullptr);
  ASSERT_EQ(TransactionType::SINGLE_HOME, forwarded_txn->internal().type());
  ASSERT_EQ(DEFAULT_MASTER_REGION_OF_NEW_KEY, forwarded_txn->keys().at("NEW").metadata().master());
  ASSERT_EQ(0U, forwarded_txn->keys().at("NEW").metadata().counter());
  ASSERT_EQ(DEFAULT_MASTER_REGION_OF_NEW_KEY, forwarded_txn->keys().at("KEY").metadata().master());
  ASSERT_EQ(0U, forwarded_txn->keys().at("KEY").metadata().counter());
}

TEST_F(ForwarderTest, ForwardMultiHome) {
  // This txn involves data mastered by two regions
  auto txn = MakeTransaction({{"A"}, {"C", KeyType::WRITE}});
  auto leader = configs[0]->leader_partition_for_multi_home_ordering();
  auto non_leader = (1 + leader) % configs[0]->num_partitions();

  // In replica 0, send to the partition that is not in charge of ordering multi-home txns
  test_slogs[non_leader]->SendTxn(txn);

  auto forwarded_txn = ReceiveOnOrdererChannel(leader);
  // The txn should be forwarded to the multihome orderer module of the leader
  ASSERT_TRUE(forwarded_txn != nullptr);
  ASSERT_EQ(TransactionType::MULTI_HOME_OR_LOCK_ONLY, forwarded_txn->internal().type());
  ASSERT_EQ(-1, forwarded_txn->internal().home());
  ASSERT_EQ(0U, forwarded_txn->keys().at("A").metadata().master());
  ASSERT_EQ(0U, forwarded_txn->keys().at("A").metadata().counter());
  ASSERT_EQ(1U, forwarded_txn->keys().at("C").metadata().master());
  ASSERT_EQ(1U, forwarded_txn->keys().at("C").metadata().counter());
}
