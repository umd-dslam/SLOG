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
  void SetUp() {
    ConfigVec configs = MakeTestConfigurations(
        "forwarder", 2 /* num_replicas */, 2 /* num_partitions */);

    for (int i = 0; i < 4; i++) {
      test_slogs_[i] = make_unique<TestSlog>(configs[i]);
      test_slogs_[i]->AddServerAndClient();
      test_slogs_[i]->AddForwarder();
      sinks_[i].reset(test_slogs_[i]->AddChannel(SEQUENCER_CHANNEL));
    }
    // Replica 0
    test_slogs_[0]->Data("A", {"vzxcv", 0, 0});
    test_slogs_[0]->Data("B", {"fbczx", 1, 1});
    test_slogs_[1]->Data("C", {"bzxcv", 0, 1});
    test_slogs_[1]->Data("D", {"naeqw", 1, 0});
    // Replica 1
    test_slogs_[2]->Data("A", {"vzxcv", 0, 0});
    test_slogs_[2]->Data("B", {"fbczx", 1, 1});
    test_slogs_[3]->Data("C", {"bzxcv", 0, 1});
    test_slogs_[3]->Data("D", {"naeqw", 1, 0});

    for (const auto& test_slog : test_slogs_) {
      test_slog->StartInNewThreads();
    }
   }

  void Receive(MMessage& msg, vector<size_t> indices) {
    CHECK(!indices.empty());
    vector<zmq::pollitem_t> poll_items;
    for (auto i : indices) {
      poll_items.push_back(sinks_[i]->GetPollItem());
    }
    auto rc = zmq::poll(poll_items, 1000);
    ASSERT_GT(rc, 0) << "Polling timed out";
    for (size_t i = 0; i < poll_items.size(); i++) {
      if (poll_items[i].revents & ZMQ_POLLIN) {
        auto& sink = sinks_[indices[i]];
        sink->Receive(msg);
        return;
      }
    }
   }

  unique_ptr<TestSlog> test_slogs_[4];
  unique_ptr<Channel> sinks_[4];
};

TEST_F(ForwarderTest, ForwardToSameRegion) {
  // This txn needs to lookup from both partitions in a region
  auto txn = MakeTransaction(
      {"A"},            /* read_set */
      {{"C", "zxcvb"}}  /* write_set */);
  // Send to partition 0 of replica 0
  test_slogs_[0]->SendTxn(txn);

  MMessage msg;
  // The txn should be forwarded to the scheduler of the same machine
  Receive(msg, {0});

  internal::Request req;
  ASSERT_TRUE(msg.GetProto(req));
  ASSERT_TRUE(req.has_forward_txn());
  const auto& forwarded_txn = req.forward_txn().txn();
  ASSERT_EQ(
      TransactionType::SINGLE_HOME, forwarded_txn.internal().type());
  const auto& master_metadata =
      forwarded_txn.internal().master_metadata();
  ASSERT_EQ(0, master_metadata.at("A").master());
  ASSERT_EQ(0, master_metadata.at("A").counter());
  ASSERT_EQ(0, master_metadata.at("C").master());
  ASSERT_EQ(1, master_metadata.at("C").counter());
}

TEST_F(ForwarderTest, ForwardToSameRegionKnownMaster) {
  auto txn = MakeTransaction(
      {"A"},            /* read_set*/
      {{"C", "bzxcv"}},  /* write_set */
      "",               /* code */
      {{"A", {0, 0}},   /* master_metadata */
       {"C", {0, 1}}});
  // Send to partition 0 of replica 0
  test_slogs_[0]->SendTxn(txn);

  MMessage msg;
  // The txn should be forwarded to the scheduler of the same machine
  Receive(msg, {0});
  ASSERT_GT(msg.Size(), 0);
  internal::Request req;
  ASSERT_TRUE(msg.GetProto(req));
  ASSERT_TRUE(req.has_forward_txn());
  const auto& forwarded_txn = req.forward_txn().txn();
  ASSERT_EQ(
      TransactionType::SINGLE_HOME, forwarded_txn.internal().type());
  const auto& master_metadata =
      forwarded_txn.internal().master_metadata();
  ASSERT_EQ(0, master_metadata.at("A").master());
  ASSERT_EQ(0, master_metadata.at("A").counter());
  ASSERT_EQ(0, master_metadata.at("C").master());
  ASSERT_EQ(1, master_metadata.at("C").counter());
}

TEST_F(ForwarderTest, ForwardToAnotherRegion) {
  // Send to partition 1 of replica 0. This txn needs to lookup
  // from both partitions and later forwarded to replica 1
  test_slogs_[1]->SendTxn(
      MakeTransaction(
          {"B"},            /* read_set */
          {{"D", "hrtjd"}}  /* write_set */));

  // Send to partition 0 of replica 1. This txn needs to lookup
  // from partition 0 only and later forwarded to replica 0
  test_slogs_[2]->SendTxn(
      MakeTransaction({"A"} /* read_set */, {}));

  {
    MMessage msg;
    internal::Request req;
    // A txn should be forwarded to one of the two schedulers in
    // replica 1
    Receive(msg, {2, 3});
    ASSERT_GT(msg.Size(), 0);
    ASSERT_TRUE(msg.GetProto(req));
    ASSERT_TRUE(req.has_forward_txn());
    const auto& forwarded_txn = req.forward_txn().txn();
    ASSERT_EQ(
        TransactionType::SINGLE_HOME, forwarded_txn.internal().type());
    const auto& master_metadata =
        forwarded_txn.internal().master_metadata();
    ASSERT_EQ(1, master_metadata.at("B").master());
    ASSERT_EQ(1, master_metadata.at("B").counter());
    ASSERT_EQ(1, master_metadata.at("D").master());
    ASSERT_EQ(0, master_metadata.at("D").counter());
  }

  {
    MMessage msg;
    internal::Request req;
    // A txn should be forwarded to one of the two schedulers in
    // replica 0
    Receive(msg, {0, 1});
    ASSERT_GT(msg.Size(), 0);
    ASSERT_TRUE(msg.GetProto(req));
    ASSERT_TRUE(req.has_forward_txn());
    const auto& forwarded_txn = req.forward_txn().txn();
    ASSERT_EQ(
        TransactionType::SINGLE_HOME, forwarded_txn.internal().type());
    const auto& master_metadata =
        forwarded_txn.internal().master_metadata();
    ASSERT_EQ(0, master_metadata.at("A").master());
    ASSERT_EQ(0, master_metadata.at("A").counter());
  }
}
