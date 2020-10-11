#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/test_utils.h"
#include "common/proto_utils.h"
#include "connection/broker.h"
#include "proto/api.pb.h"
#include "module/server.h"
#include "module/forwarder.h"
#include "storage/mem_only_storage.h"

using namespace std;
using namespace slog;

class ForwarderTest : public ::testing::Test {
protected:
  static const size_t NUM_MACHINES = 4;

  void SetUp() {
    configs = MakeTestConfigurations(
        "forwarder", 2 /* num_replicas */, 2 /* num_partitions */);

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
      poll_items.push_back(
          test_slogs[i]->GetPollItemForChannel(kSequencerChannel));
    }
    auto rc = zmq::poll(poll_items, 1000);
    if (rc == 0) return nullptr;

    for (size_t i = 0; i < poll_items.size(); i++) {
      if (poll_items[i].revents & ZMQ_POLLIN) {
        internal::Request req;
        if (!test_slogs[indices[i]]->ReceiveFromOutputChannel(req, kSequencerChannel)) {
          return nullptr;
        }
        return ExtractTxn(req);
      }
    }
    return nullptr;
  }

  Transaction* ReceiveOnOrdererChannel(size_t index) {
    internal::Request req;
    if (!test_slogs[index]->ReceiveFromOutputChannel(req, kMultiHomeOrdererChannel)) {
      return nullptr;
    }
    return ExtractTxn(req);
  }

  unique_ptr<TestSlog> test_slogs[NUM_MACHINES];
  ConfigVec configs;

private:

  Transaction* ExtractTxn(internal::Request& req) {
    if (req.type_case() != internal::Request::kForwardTxn) {
      return nullptr;
    }
    return req.mutable_forward_txn()->release_txn();
  }
};

TEST_F(ForwarderTest, ForwardToSameRegion) {
  // This txn needs to lookup from both partitions in a region
  auto txn = MakeTransaction({"A"} /* read_set */, {"B"}  /* write_set */);
  // Send to partition 0 of replica 0
  test_slogs[0]->SendTxn(txn);
  auto forwarded_txn = ReceiveOnSequencerChannel({0});
  // The txn should be forwarded to the sequencer of the same machine
  if (forwarded_txn == nullptr) {
    FAIL() << "Message was not received before timing out";
  }

  ASSERT_EQ(
      TransactionType::SINGLE_HOME, forwarded_txn->internal().type());
  const auto& master_metadata =
      forwarded_txn->internal().master_metadata();
  ASSERT_EQ(2U, master_metadata.size());
  ASSERT_EQ(0U, master_metadata.at("A").master());
  ASSERT_EQ(0U, master_metadata.at("A").counter());
  ASSERT_EQ(0U, master_metadata.at("B").master());
  ASSERT_EQ(1U, master_metadata.at("B").counter());
}

TEST_F(ForwarderTest, ForwardToAnotherRegion) {
  // Send to partition 1 of replica 0. This txn needs to lookup
  // from both partitions and later forwarded to replica 1
  test_slogs[1]->SendTxn(
      MakeTransaction({"C"} /* read_set */, {"X"}  /* write_set */));

  // Send to partition 0 of replica 1. This txn needs to lookup
  // from partition 0 only and later forwarded to replica 0
  test_slogs[2]->SendTxn(
      MakeTransaction({"A"} /* read_set */, {}));

  {
    auto forwarded_txn = ReceiveOnSequencerChannel({2, 3});
    // A txn should be forwarded to one of the two schedulers in
    // replica 1
    if (forwarded_txn == nullptr) {
      FAIL() << "Message was not received before timing out";
    }
    ASSERT_EQ(
        TransactionType::SINGLE_HOME, forwarded_txn->internal().type());
    const auto& master_metadata = forwarded_txn->internal().master_metadata();
    ASSERT_EQ(2U, master_metadata.size());
    ASSERT_EQ(1U, master_metadata.at("C").master());
    ASSERT_EQ(1U, master_metadata.at("C").counter());
    ASSERT_EQ(1U, master_metadata.at("X").master());
    ASSERT_EQ(0U, master_metadata.at("X").counter());
  }

  {
    auto forwarded_txn = ReceiveOnSequencerChannel({0, 1});
    // A txn should be forwarded to one of the two schedulers in
    // replica 0
    if (forwarded_txn == nullptr) {
      FAIL() << "Message was not received before timing out";
    }
    ASSERT_EQ(
        TransactionType::SINGLE_HOME, forwarded_txn->internal().type());
    const auto& master_metadata = forwarded_txn->internal().master_metadata();
    ASSERT_EQ(1U, master_metadata.size());
    ASSERT_EQ(0U, master_metadata.at("A").master());
    ASSERT_EQ(0U, master_metadata.at("A").counter());
  }
}

TEST_F(ForwarderTest, TransactionHasNewKeys) {
  // This txn needs to lookup from both partitions in a region
  auto txn = MakeTransaction({"NEW"} /* read_set */, {"KEY"} /* write_set */);
  // Send to partition 0 of replica 0
  test_slogs[3]->SendTxn(txn);

  auto forwarded_txn = ReceiveOnSequencerChannel({0, 1, 2, 3});
  // The txn should be forwarded to the scheduler of the same machine
  if (forwarded_txn == nullptr) {
    FAIL() << "Message was not received before timing out";
  }
  ASSERT_EQ(
      TransactionType::SINGLE_HOME, forwarded_txn->internal().type());
  const auto& master_metadata = forwarded_txn->internal().master_metadata();
  ASSERT_EQ(2U, master_metadata.size());
  ASSERT_EQ(DEFAULT_MASTER_REGION_OF_NEW_KEY, master_metadata.at("NEW").master());
  ASSERT_EQ(0U, master_metadata.at("NEW").counter());
  ASSERT_EQ(DEFAULT_MASTER_REGION_OF_NEW_KEY, master_metadata.at("KEY").master());
  ASSERT_EQ(0U, master_metadata.at("KEY").counter());
}

TEST_F(ForwarderTest, ForwardMultiHome) {
  // This txn involves data mastered by two regions
  auto txn = MakeTransaction({"A"} /* read_set */, {"C"}  /* write_set */);
  auto leader = configs[0]->leader_partition_for_multi_home_ordering();
  auto non_leader = (1 + leader) % configs[0]->num_partitions();  

  // In replica 0, send to the partition that is not in charge of ordering multi-home txns
  test_slogs[non_leader]->SendTxn(txn);

  auto forwarded_txn = ReceiveOnOrdererChannel(leader);
  // The txn should be forwarded to the multihome orderer module of the leader
  if (forwarded_txn == nullptr) {
    FAIL() << "Message was not received before timing out";
  }

  ASSERT_EQ(
      TransactionType::MULTI_HOME, forwarded_txn->internal().type());
  const auto& master_metadata =
      forwarded_txn->internal().master_metadata();
  ASSERT_EQ(2U, master_metadata.size());
  ASSERT_EQ(0U, master_metadata.at("A").master());
  ASSERT_EQ(0U, master_metadata.at("A").counter());
  ASSERT_EQ(1U, master_metadata.at("C").master());
  ASSERT_EQ(1U, master_metadata.at("C").counter());
}
