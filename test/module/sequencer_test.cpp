#include <gtest/gtest.h>

#include <vector>

#include "common/proto_utils.h"
#include "test/test_utils.h"

using namespace std;
using namespace slog;

using internal::Envelope;
using internal::Request;

class SequencerTest : public ::testing::TestWithParam<bool> {
 public:
  void SetUp() {
    auto delayed = GetParam();

    if (delayed) {
      internal::Configuration extra_config;
      extra_config.mutable_replication_delay()->set_delay_pct(100);
      extra_config.mutable_replication_delay()->set_delay_amount_ms(5);
      configs_ = MakeTestConfigurations("sequencer", 2, 2, extra_config);
    } else {
      configs_ = MakeTestConfigurations("sequencer", 2, 2);
    }

    for (int i = 0; i < 4; i++) {
      slog_[i] = make_unique<TestSlog>(configs_[i]);
      slog_[i]->AddSequencer();
      slog_[i]->AddOutputSocket(kInterleaverChannel);
      slog_[i]->AddOutputSocket(kLocalLogChannel);
    }
    sender_ = slog_[0]->NewSender();
    for (auto& slog : slog_) {
      slog->StartInNewThreads();
    }
  }

  void SendToSequencer(EnvelopePtr&& req) { sender_->Send(std::move(req), kSequencerChannel); }

  internal::Batch* ReceiveBatch(int i) {
    // Iff i is the id of the sender (0), check the local log channel instead
    auto req_env = slog_[i]->ReceiveFromOutputSocket(i == 0 ? kLocalLogChannel : kInterleaverChannel);
    if (req_env == nullptr) {
      return nullptr;
    }
    if (req_env->request().type_case() != Request::kForwardBatch) {
      return nullptr;
    }
    auto forward_batch = req_env->mutable_request()->mutable_forward_batch();
    if (forward_batch->part_case() != internal::ForwardBatch::kBatchData) {
      return nullptr;
    }
    auto batch = req_env->mutable_request()->mutable_forward_batch()->release_batch_data();
    return batch;
  }

  unique_ptr<Sender> sender_;
  unique_ptr<TestSlog> slog_[4];
  ConfigVec configs_;
};

TEST_P(SequencerTest, SingleHomeTransaction) {
  // A and C are in partition 0. B is in partition 1
  auto txn = MakeTestTransaction(configs_[0], 1000,
                                 {{"A", KeyType::READ, 0}, {"B", KeyType::READ, 0}, {"C", KeyType::WRITE, 0}});

  auto env = make_unique<Envelope>();
  env->mutable_request()->mutable_forward_txn()->mutable_txn()->CopyFrom(*txn);
  SendToSequencer(move(env));

  for (int rep = 0; rep < 2; rep++) {
    {
      auto batch = ReceiveBatch(configs_[0]->MakeMachineId(rep, 0));
      ASSERT_NE(batch, nullptr);
      ASSERT_EQ(batch->transactions_size(), 1);
      ASSERT_EQ(batch->transaction_type(), TransactionType::SINGLE_HOME);
      auto& batched_txn = batch->transactions().at(0);
      ASSERT_EQ(batched_txn.internal().id(), 1000);
      ASSERT_EQ(batched_txn.keys_size(), 2);
      ASSERT_EQ(batched_txn.keys().at("A"), txn->keys().at("A"));
      ASSERT_EQ(batched_txn.keys().at("C"), txn->keys().at("C"));
      delete batch;
    }

    {
      auto batch = ReceiveBatch(configs_[0]->MakeMachineId(rep, 1));
      ASSERT_NE(batch, nullptr);
      ASSERT_EQ(batch->transactions_size(), 1);
      ASSERT_EQ(batch->transaction_type(), TransactionType::SINGLE_HOME);
      auto& batched_txn = batch->transactions().at(0);
      ASSERT_EQ(batched_txn.internal().id(), 1000);
      ASSERT_EQ(batched_txn.keys_size(), 1);
      ASSERT_EQ(batched_txn.keys().at("B"), txn->keys().at("B"));
      delete batch;
    }
  }
}

TEST_P(SequencerTest, MultiHomeTransaction1) {
  //             A  B  C  D
  // Partition:  0  1  0  1
  auto txn = MakeTestTransaction(
      configs_[0], 1000,
      {{"A", KeyType::READ, 0}, {"B", KeyType::READ, 1}, {"C", KeyType::WRITE, 0}, {"D", KeyType::WRITE, 0}});
  auto env = make_unique<Envelope>();
  env->mutable_request()->mutable_forward_txn()->mutable_txn()->CopyFrom(*txn);

  SendToSequencer(move(env));

  // The txn was sent to region 0, which generates two subtxns, each for each partitions. These subtxns are
  // also replicated to region 1.
  for (int rep = 0; rep < 2; rep++) {
    {
      auto batch = ReceiveBatch(configs_[0]->MakeMachineId(rep, 0));

      ASSERT_NE(batch, nullptr);
      ASSERT_EQ(batch->transactions_size(), 1);

      auto lo_txn = batch->transactions().at(0);
      ASSERT_EQ(lo_txn.internal().id(), 1000);
      ASSERT_EQ(lo_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
      ASSERT_EQ(lo_txn.internal().home(), 0);
      ASSERT_EQ(lo_txn.keys_size(), 2);
      ASSERT_EQ(lo_txn.keys().at("A"), txn->keys().at("A"));
      ASSERT_EQ(lo_txn.keys().at("C"), txn->keys().at("C"));

      delete batch;
    }

    {
      auto batch = ReceiveBatch(configs_[0]->MakeMachineId(rep, 1));

      ASSERT_NE(batch, nullptr);
      ASSERT_EQ(batch->transactions_size(), 1);

      auto lo_txn = batch->transactions().at(0);
      ASSERT_EQ(lo_txn.internal().id(), 1000);
      ASSERT_EQ(lo_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
      ASSERT_EQ(lo_txn.internal().home(), 0);
      ASSERT_EQ(lo_txn.keys_size(), 2);
      ASSERT_EQ(lo_txn.keys().at("B"), txn->keys().at("B"));
      ASSERT_EQ(lo_txn.keys().at("D"), txn->keys().at("D"));

      delete batch;
    }
  }
}

TEST_P(SequencerTest, MultiHomeTransaction2) {
  //             A  B  C  D
  // Partition:  0  1  0  1
  auto txn = MakeTestTransaction(
      configs_[0], 1000,
      {{"A", KeyType::READ, 1}, {"B", KeyType::READ, 0}, {"C", KeyType::WRITE, 1}, {"D", KeyType::WRITE, 1}});
  auto env = make_unique<Envelope>();
  env->mutable_request()->mutable_forward_txn()->mutable_txn()->CopyFrom(*txn);

  SendToSequencer(move(env));

  // The txn was sent to region 0, which only generates one subtxn for partition 1 because the subtxn for partition
  // 0 is redundant (both A and C are homed at region 1)
  for (int rep = 0; rep < 2; rep++) {
    {
      auto batch = ReceiveBatch(configs_[0]->MakeMachineId(rep, 0));

      ASSERT_NE(batch, nullptr);
      ASSERT_EQ(batch->transactions_size(), 0);

      delete batch;
    }

    {
      auto batch = ReceiveBatch(configs_[0]->MakeMachineId(rep, 1));

      ASSERT_NE(batch, nullptr);
      ASSERT_EQ(batch->transactions_size(), 1);

      auto lo_txn = batch->transactions().at(0);
      ASSERT_EQ(lo_txn.internal().id(), 1000);
      ASSERT_EQ(lo_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
      ASSERT_EQ(lo_txn.internal().home(), 0);
      ASSERT_EQ(lo_txn.keys_size(), 2);
      ASSERT_EQ(lo_txn.keys().at("B"), txn->keys().at("B"));
      ASSERT_EQ(lo_txn.keys().at("D"), txn->keys().at("D"));

      delete batch;
    }
  }
}

#ifdef REMASTER_PROTOCOL_COUNTERLESS
TEST_P(SequencerTest, RemasterTransaction) {
  auto txn = MakeTestTransaction(configs_[0], 1000, {{"A", KeyType::WRITE, 1}}, 0);
  auto env = make_unique<Envelope>();
  env->mutable_request()->mutable_forward_txn()->mutable_txn()->CopyFrom(*txn);

  SendToSequencer(move(env));

  for (int rep = 0; rep < 2; rep++) {
    auto batch = ReceiveBatch(configs_[0]->MakeMachineId(rep, 0));

    ASSERT_NE(batch, nullptr);
    ASSERT_EQ(batch->transactions_size(), 1);

    auto lo_txn = batch->transactions().at(0);
    ASSERT_EQ(lo_txn.internal().id(), 1000);
    ASSERT_EQ(lo_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
    ASSERT_EQ(lo_txn.internal().home(), 0);
    ASSERT_EQ(lo_txn.keys_size(), 1);
    ASSERT_EQ(lo_txn.keys().at("A"), txn->keys().at("A"));
    ASSERT_TRUE(lo_txn.remaster().is_new_master_lock_only());

    delete batch;
  }
}
#endif

TEST_P(SequencerTest, MultiHomeTransactionBypassedOrderer) {
  // "A" and "B" are on two different partitions
  auto txn = MakeTestTransaction(configs_[0], 1000, {{"A", KeyType::READ, 0}, {"B", KeyType::READ, 1}});

  auto env = make_unique<Envelope>();
  env->mutable_request()->mutable_forward_txn()->mutable_txn()->CopyFrom(*txn);

  SendToSequencer(move(env));

  // The txn was sent to region 0, which only generates one subtxns for partition 0 because B of partition 1
  // does not have any key homed at 0.
  for (int rep = 0; rep < 2; rep++) {
    {
      auto batch = ReceiveBatch(configs_[0]->MakeMachineId(rep, 0));

      ASSERT_NE(batch, nullptr);
      ASSERT_EQ(batch->transactions_size(), 1);

      auto lo_txn = batch->transactions().at(0);
      ASSERT_EQ(lo_txn.internal().id(), 1000);
      ASSERT_EQ(lo_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
      ASSERT_EQ(lo_txn.internal().home(), 0);
      ASSERT_EQ(lo_txn.keys_size(), 1);
      ASSERT_EQ(lo_txn.keys().at("A"), txn->keys().at("A"));

      delete batch;
    }

    {
      auto batch = ReceiveBatch(configs_[0]->MakeMachineId(rep, 1));

      ASSERT_NE(batch, nullptr);
      ASSERT_EQ(batch->transactions_size(), 0);

      delete batch;
    }
  }
}

INSTANTIATE_TEST_SUITE_P(AllSequencerTests, SequencerTest, testing::Values(false, true),
                         [](const testing::TestParamInfo<bool>& info) {
                           return info.param ? "Delayed" : "NotDelayed";
                         });