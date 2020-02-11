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

class E2ETest : public ::testing::Test {
protected:
  static const size_t NUM_MACHINES = 1;

  void SetUp() {
    configs = MakeTestConfigurations(
        "e2e", 1 /* num_replicas */, 1 /* num_partitions */);

    for (size_t i = 0; i < NUM_MACHINES; i++) {
      test_slogs[i] = make_unique<TestSlog>(configs[i]);

      test_slogs[i]->AddServerAndClient();
      test_slogs[i]->AddForwarder();
      test_slogs[i]->AddSequencer();
      test_slogs[i]->AddScheduler();
      test_slogs[i]->AddLocalPaxos();
    }
    // // Replica 0
    test_slogs[0]->Data("A", {"xxxxx", 0, 0});

    for (const auto& test_slog : test_slogs) {
      test_slog->StartInNewThreads();
    }
  }

  unique_ptr<TestSlog> test_slogs[NUM_MACHINES];
  ConfigVec configs;
};

TEST_F(E2ETest, BasicSingleHomeSingleParition) {
  auto txn = MakeTransaction({"A"} /* read_set */, {}  /* write_set */);
  test_slogs[0]->SendTxn(txn);
  auto txn_resp = test_slogs[0]->RecvTxnResult();
  ASSERT_EQ(TransactionStatus::COMMITTED, txn_resp.status());
}
