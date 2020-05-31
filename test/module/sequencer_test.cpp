#include <vector>

#include <gtest/gtest.h>

#include "common/test_utils.h"
#include "common/proto_utils.h"

using namespace std;
using namespace slog;

using internal::Request;

class SequencerTest : public ::testing::Test {
public:
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

  unique_ptr<Channel> input_;
  unique_ptr<Channel> output_;
  unique_ptr<TestSlog> slog_;
};

#ifdef ENABLE_REPLICATION_DELAY
class SequencerReplicationDelayTest : public SequencerTest {
public:
  void SetUp() {}
  void CustomSetUp(uint32_t delay_percent, uint32_t delay_amount) {
    internal::Configuration extra_config;
    extra_config.mutable_replication_delay()->set_batch_delay_percent(delay_percent);
    extra_config.mutable_replication_delay()->set_batch_delay_amount(delay_amount);
    auto configs = MakeTestConfigurations("sequencer_replication_delay", 2, 1, 0, extra_config);
    slog_ = make_unique<TestSlog>(configs[0]);
    auto slog2 = make_unique<TestSlog>(configs[1]);
    slog_->AddSequencer();
    input_ = slog_->AddChannel(FORWARDER_CHANNEL);
    output_ = slog_->AddChannel(SCHEDULER_CHANNEL);
    slog_->StartInNewThreads();
    slog2->StartInNewThreads();
  }
};
#endif /* ENABLE_REPLICATION_DELAY */

TEST_F(SequencerTest, SingleHomeTransaction) {
  auto txn = MakeTransaction(
      {"A", "B"},
      {"C"},
      "some code",
      {{"A", {0, 0}}, {"B", {0, 0}}, {"C", {0, 0}}});

  Request req;
  req.mutable_forward_txn()->mutable_txn()->CopyFrom(*txn);

  SendToSequencer(req);

  auto batch = ReceiveBatch();
  ASSERT_NE(batch, nullptr);
  ASSERT_EQ(batch->transactions_size(), 1);
  ASSERT_EQ(batch->transactions().at(0), *txn);
  ASSERT_EQ(batch->transaction_type(), TransactionType::SINGLE_HOME);

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
  mh_batch->add_transactions()->CopyFrom(*txn1);
  mh_batch->add_transactions()->CopyFrom(*txn2);
  mh_batch->set_transaction_type(TransactionType::MULTI_HOME);
  SendToSequencer(req);

  for (int i = 0; i < 2; i++) {
    auto batch = ReceiveBatch();
    ASSERT_NE(batch, nullptr);

    switch (batch->transaction_type()) {
      case TransactionType::SINGLE_HOME: {
        ASSERT_EQ(batch->transactions_size(), 2);

        auto sh_txn1 = batch->transactions().at(0);
        ASSERT_EQ(sh_txn1.read_set_size(), 1);
        ASSERT_TRUE(sh_txn1.read_set().contains("A"));
        ASSERT_EQ(sh_txn1.write_set_size(), 0);
        ASSERT_EQ(sh_txn1.internal().type(), TransactionType::LOCK_ONLY);

        auto sh_txn2 = batch->transactions().at(1);
        ASSERT_EQ(sh_txn2.read_set_size(), 0);
        ASSERT_EQ(sh_txn2.write_set_size(), 1);
        ASSERT_TRUE(sh_txn2.write_set().contains("D"));
        ASSERT_EQ(sh_txn2.internal().type(), TransactionType::LOCK_ONLY);
        break;
      }
      case TransactionType::MULTI_HOME: {
        ASSERT_EQ(batch->transactions_size(), 2);

        auto mh_txn1 = batch->transactions().at(0);
        ASSERT_EQ(mh_txn1, *txn1);
        
        auto mh_txn2 = batch->transactions().at(1);
        ASSERT_EQ(mh_txn2, *txn2);
        break;
      }
      default:
        FAIL() << "Wrong transaction type. Expected SINGLE_HOME or MULTI_HOME. Actual: " 
               << ENUM_NAME(batch->transaction_type(), TransactionType);
        break;
    }
    delete batch;
  }
}

#ifdef ENABLE_REPLICATION_DELAY
TEST_F(SequencerReplicationDelayTest, SingleHomeTransaction) {
  CustomSetUp(100, 3);
  auto txn = MakeTransaction(
      {"A", "B"},
      {"C"},
      "some code",
      {{"A", {0, 0}}, {"B", {0, 0}}, {"C", {0, 0}}});

  Request req;
  req.mutable_forward_txn()->mutable_txn()->CopyFrom(*txn);

  SendToSequencer(req);

  {
    MMessage msg;
    output_->Receive(msg);
    Request req;
    ASSERT_TRUE(msg.GetProto(req));
    ASSERT_EQ(req.type_case(), Request::kForwardBatch);
    auto forward_batch = req.mutable_forward_batch();
    ASSERT_EQ(forward_batch->part_case(), internal::ForwardBatch::kBatchData);
    auto batch = req.mutable_forward_batch()->release_batch_data();
    ASSERT_EQ(batch->transactions_size(), 1);
    ASSERT_EQ(batch->transactions().at(0), *txn);
    ASSERT_EQ(batch->transaction_type(), TransactionType::SINGLE_HOME);
  }
  {
    MMessage msg;
    output_->Receive(msg);
    Request req;
    ASSERT_TRUE(msg.GetProto(req));
    ASSERT_EQ(req.type_case(), Request::kForwardBatch);
    auto forward_batch = req.mutable_forward_batch();
    ASSERT_EQ(forward_batch->part_case(), internal::ForwardBatch::kBatchData);
    auto batch = req.mutable_forward_batch()->release_batch_data();
    ASSERT_EQ(batch->transactions_size(), 1);
    ASSERT_EQ(batch->transactions().at(0), *txn);
    ASSERT_EQ(batch->transaction_type(), TransactionType::SINGLE_HOME);
  }
}
#endif /* ENABLE_REPLICATION_DELAY */