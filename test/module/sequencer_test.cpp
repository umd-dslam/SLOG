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
      senders_[i] = slog_[i]->NewSender();
    }
    for (auto& slog : slog_) {
      slog->StartInNewThreads();
    }
  }

  void SendToSequencer(int i, EnvelopePtr&& req) { senders_[i]->Send(std::move(req), kSequencerChannel); }

  vector<internal::Batch> ReceiveBatches(int i, bool is_local_log) {
    auto channel = is_local_log ? kLocalLogChannel : kInterleaverChannel;
    auto req_env = slog_[i]->ReceiveFromOutputSocket(channel);
    if (req_env == nullptr) {
      return {};
    }
    if (req_env->request().type_case() != Request::kForwardBatchData) {
      return {};
    }
    auto forward_batch = req_env->mutable_request()->mutable_forward_batch_data();
    vector<internal::Batch> batches;
    for (int i = 0; i < forward_batch->batch_data_size(); i++) {
      batches.push_back(forward_batch->batch_data(i));
    }
    while (!forward_batch->batch_data().empty()) {
      forward_batch->mutable_batch_data()->ReleaseLast();
    }
    return batches;
  }

  unique_ptr<Sender> senders_[4];
  unique_ptr<TestSlog> slog_[4];
  ConfigVec configs_;
};

TEST_P(SequencerTest, SingleHomeTransaction1) {
  // A and C are in partition 0. B is in partition 1
  auto txn = MakeTestTransaction(configs_[0], 1000,
                                 {{"A", KeyType::READ, 0}, {"B", KeyType::READ, 0}, {"C", KeyType::WRITE, 0}});

  auto env = make_unique<Envelope>();
  env->mutable_request()->mutable_forward_txn()->mutable_txn()->CopyFrom(*txn);
  SendToSequencer(0, move(env));

  // Batch partitions are distributed to corresponding local partitions
  {
    auto batches = ReceiveBatches(0, true);
    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0].transactions_size(), 1);
    ASSERT_EQ(batches[0].transaction_type(), TransactionType::SINGLE_HOME);
    auto& batched_txn = batches[0].transactions().at(0);
    ASSERT_EQ(batched_txn.internal().id(), 1000);
    ASSERT_EQ(batched_txn.keys_size(), 2);
    ASSERT_EQ(TxnValueEntry(batched_txn, "A"), TxnValueEntry(*txn, "A"));
    ASSERT_EQ(TxnValueEntry(batched_txn, "C"), TxnValueEntry(*txn, "C"));
  }

  {
    auto batches = ReceiveBatches(1, true);
    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0].transactions_size(), 1);
    ASSERT_EQ(batches[0].transaction_type(), TransactionType::SINGLE_HOME);
    auto& batched_txn = batches[0].transactions().at(0);
    ASSERT_EQ(batched_txn.internal().id(), 1000);
    ASSERT_EQ(batched_txn.keys_size(), 1);
    ASSERT_EQ(TxnValueEntry(batched_txn, "B"), TxnValueEntry(*txn, "B"));
  }

  // All batch partitions are sent to a machine in the remote replica
  {
    auto batches = ReceiveBatches(3, false);
    ASSERT_EQ(batches.size(), 2);
    ASSERT_EQ(batches[0].transactions_size(), 1);
    ASSERT_EQ(batches[0].transaction_type(), TransactionType::SINGLE_HOME);
    {
      auto& batched_txn = batches[0].transactions().at(0);
      ASSERT_EQ(batched_txn.internal().id(), 1000);
      ASSERT_EQ(batched_txn.keys_size(), 2);
      ASSERT_EQ(TxnValueEntry(batched_txn, "A"), TxnValueEntry(*txn, "A"));
      ASSERT_EQ(TxnValueEntry(batched_txn, "C"), TxnValueEntry(*txn, "C"));
    }
    ASSERT_EQ(batches[1].transactions_size(), 1);
    ASSERT_EQ(batches[1].transaction_type(), TransactionType::SINGLE_HOME);
    {
      auto& batched_txn = batches[1].transactions().at(0);
      ASSERT_EQ(batched_txn.internal().id(), 1000);
      ASSERT_EQ(batched_txn.keys_size(), 1);
      ASSERT_EQ(TxnValueEntry(batched_txn, "B"), TxnValueEntry(*txn, "B"));
    }
  }
}

TEST_P(SequencerTest, SingleHomeTransaction2) {
  // A and C are in partition 0. B is in partition 1
  auto txn = MakeTestTransaction(configs_[0], 1000,
                                 {{"A", KeyType::READ, 1}, {"B", KeyType::READ, 1}, {"C", KeyType::WRITE, 1}});

  auto env = make_unique<Envelope>();
  env->mutable_request()->mutable_forward_txn()->mutable_txn()->CopyFrom(*txn);
  SendToSequencer(2, move(env));

  // Batch partitions are distributed to corresponding local partitions
  {
    auto batches = ReceiveBatches(2, true);
    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0].transactions_size(), 1);
    ASSERT_EQ(batches[0].transaction_type(), TransactionType::SINGLE_HOME);
    auto& batched_txn = batches[0].transactions().at(0);
    ASSERT_EQ(batched_txn.internal().id(), 1000);
    ASSERT_EQ(batched_txn.keys_size(), 2);
    ASSERT_EQ(TxnValueEntry(batched_txn, "A"), TxnValueEntry(*txn, "A"));
    ASSERT_EQ(TxnValueEntry(batched_txn, "C"), TxnValueEntry(*txn, "C"));
  }

  {
    auto batches = ReceiveBatches(3, true);
    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0].transactions_size(), 1);
    ASSERT_EQ(batches[0].transaction_type(), TransactionType::SINGLE_HOME);
    auto& batched_txn = batches[0].transactions().at(0);
    ASSERT_EQ(batched_txn.internal().id(), 1000);
    ASSERT_EQ(batched_txn.keys_size(), 1);
    ASSERT_EQ(TxnValueEntry(batched_txn, "B"), TxnValueEntry(*txn, "B"));
  }

  // All batch partitions are sent to a machine in the remote replica
  {
    auto batches = ReceiveBatches(1, false);
    ASSERT_EQ(batches.size(), 2);
    ASSERT_EQ(batches[0].transactions_size(), 1);
    ASSERT_EQ(batches[0].transaction_type(), TransactionType::SINGLE_HOME);
    {
      auto& batched_txn = batches[0].transactions().at(0);
      ASSERT_EQ(batched_txn.internal().id(), 1000);
      ASSERT_EQ(batched_txn.keys_size(), 2);
      ASSERT_EQ(TxnValueEntry(batched_txn, "A"), TxnValueEntry(*txn, "A"));
      ASSERT_EQ(TxnValueEntry(batched_txn, "C"), TxnValueEntry(*txn, "C"));
    }
    ASSERT_EQ(batches[1].transactions_size(), 1);
    ASSERT_EQ(batches[1].transaction_type(), TransactionType::SINGLE_HOME);
    {
      auto& batched_txn = batches[1].transactions().at(0);
      ASSERT_EQ(batched_txn.internal().id(), 1000);
      ASSERT_EQ(batched_txn.keys_size(), 1);
      ASSERT_EQ(TxnValueEntry(batched_txn, "B"), TxnValueEntry(*txn, "B"));
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

  SendToSequencer(0, move(env));

  // The txn was sent to region 0, which generates two subtxns, each for each partitions. These subtxns are
  // also replicated to region 1.
  {
    auto batches = ReceiveBatches(0, true);
    ASSERT_EQ(batches.size(), 1);
    auto lo_txn = batches[0].transactions().at(0);
    ASSERT_EQ(lo_txn.internal().id(), 1000);
    ASSERT_EQ(lo_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
    ASSERT_EQ(lo_txn.internal().home(), 0);
    ASSERT_EQ(lo_txn.keys_size(), 2);
    ASSERT_EQ(TxnValueEntry(lo_txn, "A"), TxnValueEntry(*txn, "A"));
    ASSERT_EQ(TxnValueEntry(lo_txn, "C"), TxnValueEntry(*txn, "C"));
  }

  {
    auto batches = ReceiveBatches(1, true);
    ASSERT_EQ(batches.size(), 1);
    auto lo_txn = batches[0].transactions().at(0);
    ASSERT_EQ(lo_txn.internal().id(), 1000);
    ASSERT_EQ(lo_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
    ASSERT_EQ(lo_txn.internal().home(), 0);
    ASSERT_EQ(lo_txn.keys_size(), 2);
    ASSERT_EQ(TxnValueEntry(lo_txn, "B"), TxnValueEntry(*txn, "B"));
    ASSERT_EQ(TxnValueEntry(lo_txn, "D"), TxnValueEntry(*txn, "D"));
  }

  {
    auto batches = ReceiveBatches(3, false);
    ASSERT_FALSE(batches.empty());
    ASSERT_EQ(batches.size(), 2);
    {
      auto lo_txn = batches[0].transactions().at(0);
      ASSERT_EQ(lo_txn.internal().id(), 1000);
      ASSERT_EQ(lo_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
      ASSERT_EQ(lo_txn.internal().home(), 0);
      ASSERT_EQ(lo_txn.keys_size(), 2);
      ASSERT_EQ(TxnValueEntry(lo_txn, "A"), TxnValueEntry(*txn, "A"));
      ASSERT_EQ(TxnValueEntry(lo_txn, "C"), TxnValueEntry(*txn, "C"));
    }
    {
      auto lo_txn = batches[1].transactions().at(0);
      ASSERT_EQ(lo_txn.internal().id(), 1000);
      ASSERT_EQ(lo_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
      ASSERT_EQ(lo_txn.internal().home(), 0);
      ASSERT_EQ(lo_txn.keys_size(), 2);
      ASSERT_EQ(TxnValueEntry(lo_txn, "B"), TxnValueEntry(*txn, "B"));
      ASSERT_EQ(TxnValueEntry(lo_txn, "D"), TxnValueEntry(*txn, "D"));
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

  SendToSequencer(0, move(env));

  // The txn was sent to region 0, which only generates one subtxn for partition 1 because the subtxn for partition
  // 0 is redundant (both A and C are homed at region 1)
  {
    auto batches = ReceiveBatches(0, true);
    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0].transactions_size(), 0);
  }

  {
    auto batches = ReceiveBatches(1, true);

    ASSERT_EQ(batches.size(), 1);

    auto lo_txn = batches[0].transactions().at(0);
    ASSERT_EQ(lo_txn.internal().id(), 1000);
    ASSERT_EQ(lo_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
    ASSERT_EQ(lo_txn.internal().home(), 0);
    ASSERT_EQ(lo_txn.keys_size(), 2);
    ASSERT_EQ(TxnValueEntry(lo_txn, "B"), TxnValueEntry(*txn, "B"));
    ASSERT_EQ(TxnValueEntry(lo_txn, "D"), TxnValueEntry(*txn, "D"));
  }

  {
    auto batches = ReceiveBatches(3, false);
    ASSERT_FALSE(batches.empty());
    ASSERT_EQ(batches.size(), 2);

    ASSERT_EQ(batches[0].transactions_size(), 0);

    auto lo_txn = batches[1].transactions().at(0);
    ASSERT_EQ(lo_txn.internal().id(), 1000);
    ASSERT_EQ(lo_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
    ASSERT_EQ(lo_txn.internal().home(), 0);
    ASSERT_EQ(lo_txn.keys_size(), 2);
    ASSERT_EQ(TxnValueEntry(lo_txn, "B"), TxnValueEntry(*txn, "B"));
    ASSERT_EQ(TxnValueEntry(lo_txn, "D"), TxnValueEntry(*txn, "D"));
  }
}

#ifdef REMASTER_PROTOCOL_COUNTERLESS
TEST_P(SequencerTest, RemasterTransaction) {
  auto txn = MakeTestTransaction(configs_[0], 1000, {{"A", KeyType::WRITE, 1}}, {}, 0);
  auto env = make_unique<Envelope>();
  env->mutable_request()->mutable_forward_txn()->mutable_txn()->CopyFrom(*txn);

  SendToSequencer(0, move(env));

  {
    auto batches = ReceiveBatches(0, true);

    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0].transactions_size(), 1);

    auto lo_txn = batches[0].transactions().at(0);
    ASSERT_EQ(lo_txn.internal().id(), 1000);
    ASSERT_EQ(lo_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
    ASSERT_EQ(lo_txn.internal().home(), 0);
    ASSERT_EQ(lo_txn.keys_size(), 1);
    ASSERT_EQ(TxnValueEntry(lo_txn, "A"), TxnValueEntry(*txn, "A"));
    ASSERT_TRUE(lo_txn.remaster().is_new_master_lock_only());
  }

  {
    auto batches = ReceiveBatches(1, true);

    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0].transactions_size(), 0);
  }

  {
    auto batches = ReceiveBatches(3, false);

    ASSERT_EQ(batches.size(), 2);
    ASSERT_EQ(batches[0].transactions_size(), 1);

    auto lo_txn = batches[0].transactions().at(0);
    ASSERT_EQ(lo_txn.internal().id(), 1000);
    ASSERT_EQ(lo_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
    ASSERT_EQ(lo_txn.internal().home(), 0);
    ASSERT_EQ(lo_txn.keys_size(), 1);
    ASSERT_EQ(TxnValueEntry(lo_txn, "A"), TxnValueEntry(*txn, "A"));
    ASSERT_TRUE(lo_txn.remaster().is_new_master_lock_only());

    ASSERT_EQ(batches[1].transactions_size(), 0);
  }
}
#endif

TEST_P(SequencerTest, MultiHomeTransactionBypassedOrderer) {
  // "A" and "B" are on two different partitions
  auto txn = MakeTestTransaction(configs_[0], 1000, {{"A", KeyType::READ, 0}, {"B", KeyType::READ, 1}});

  auto env = make_unique<Envelope>();
  env->mutable_request()->mutable_forward_txn()->mutable_txn()->CopyFrom(*txn);

  SendToSequencer(0, move(env));

  // The txn was sent to region 0, which only generates one subtxns for partition 0 because B of partition 1
  // does not have any key homed at 0.
  {
    auto batches = ReceiveBatches(0, true);

    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0].transactions_size(), 1);

    auto lo_txn = batches[0].transactions().at(0);
    ASSERT_EQ(lo_txn.internal().id(), 1000);
    ASSERT_EQ(lo_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
    ASSERT_EQ(lo_txn.internal().home(), 0);
    ASSERT_EQ(lo_txn.keys_size(), 1);
    ASSERT_EQ(TxnValueEntry(lo_txn, "A"), TxnValueEntry(*txn, "A"));
  }

  {
    auto batches = ReceiveBatches(1, true);

    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0].transactions_size(), 0);
  }

  {
    auto batches = ReceiveBatches(3, false);

    ASSERT_EQ(batches.size(), 2);
    ASSERT_EQ(batches[0].transactions_size(), 1);

    auto lo_txn = batches[0].transactions().at(0);
    ASSERT_EQ(lo_txn.internal().id(), 1000);
    ASSERT_EQ(lo_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
    ASSERT_EQ(lo_txn.internal().home(), 0);
    ASSERT_EQ(lo_txn.keys_size(), 1);
    ASSERT_EQ(TxnValueEntry(lo_txn, "A"), TxnValueEntry(*txn, "A"));

    ASSERT_EQ(batches[1].transactions_size(), 0);
  }
}

INSTANTIATE_TEST_SUITE_P(AllSequencerTests, SequencerTest, testing::Values(false, true),
                         [](const testing::TestParamInfo<bool>& info) {
                           return info.param ? "Delayed" : "NotDelayed";
                         });