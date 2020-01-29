#include <vector>

#include <gtest/gtest.h>

#include "common/test_utils.h"
#include "common/proto_utils.h"

using namespace std;
using namespace slog;

using internal::Request;

class SequencerTest : public ::testing::Test {
protected:
  void SetUp() {
    auto configs = MakeTestConfigurations("sequencer", 1, 1);
    slog_ = make_unique<TestSlog>(configs[0]);
    slog_->AddSequencer();
    input_ = slog_->AddChannel(FORWARDER_CHANNEL);
    output_ = slog_->AddChannel(SCHEDULER_CHANNEL);
    slog_->StartInNewThreads();
  }

  void SendToSequencer(internal::Request req) {
    MMessage msg;
    msg.Set(MM_PROTO, req);
    msg.Set(MM_TO_CHANNEL, SEQUENCER_CHANNEL);
    input_->Send(msg);
  }

  internal::Batch* ReceiveBatch() {
    MMessage msg;
    output_->Receive(msg);
    Request req;
    if (!msg.GetProto(req)) {
      return nullptr;
    }
    if (req.type_case() != Request::kForwardBatch) {
      return nullptr;
    }
    auto forward_batch = req.mutable_forward_batch();
    if (forward_batch->part_case() != internal::ForwardBatch::kBatchData) {
      return nullptr;
    }
    auto batch = req.mutable_forward_batch()->release_batch_data();
    return batch;
  }

private:
  unique_ptr<Channel> input_;
  unique_ptr<Channel> output_;
  unique_ptr<TestSlog> slog_;
};

TEST_F(SequencerTest, SingleHomeTransaction) {
  auto txn = MakeTransaction(
      {"A", "B"},
      {"C"},
      "some code",
      {{"A", {0, 0}}, {"B", {0, 0}}, {"C", {0, 0}}});

  Request req;
  req.mutable_forward_txn()->mutable_txn()->CopyFrom(txn);

  SendToSequencer(req);

  auto batch = ReceiveBatch();
  ASSERT_NE(batch, nullptr);
  ASSERT_EQ(batch->transactions_size(), 1);
  ASSERT_EQ(batch->transactions().at(0), txn);

  delete batch;
}

TEST_F(SequencerTest, MultiHomeTransaction) {
  auto txn1 = MakeTransaction(
      {"A", "B"},
      {},
      "some code",
      {{"A", {0, 0}}, {"B", {1, 0}}});
  
  auto txn2 = MakeTransaction(
      {},
      {"C", "D"},
      "some code",
      {{"C", {1, 0}}, {"D", {0, 0}}});

  Request req;
  auto mh_batch = req.mutable_forward_batch()->mutable_batch_data();
  mh_batch->add_transactions()->CopyFrom(txn1);
  mh_batch->add_transactions()->CopyFrom(txn2);
  SendToSequencer(req);

  auto sh_batch = ReceiveBatch();
  ASSERT_NE(sh_batch, nullptr);
  ASSERT_EQ(sh_batch->transactions_size(), 2);

  auto sh_txn1 = sh_batch->transactions().at(0);
  ASSERT_EQ(sh_txn1.read_set_size(), 0);
  ASSERT_EQ(sh_txn1.write_set_size(), 1);
  ASSERT_TRUE(sh_txn1.write_set().contains("D"));
  ASSERT_EQ(sh_txn1.internal().type(), TransactionType::LOCK_ONLY);

  auto sh_txn2 = sh_batch->transactions().at(1);
  ASSERT_EQ(sh_txn2.read_set_size(), 1);
  ASSERT_TRUE(sh_txn2.read_set().contains("A"));
  ASSERT_EQ(sh_txn2.write_set_size(), 0);
  ASSERT_EQ(sh_txn2.internal().type(), TransactionType::LOCK_ONLY);
  delete sh_batch;
}