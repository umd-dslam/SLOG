#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/test_utils.h"
#include "connection/broker.h"
#include "proto/internal.pb.h"
#include "connection/broker.h"
#include "module/server.h"
#include "storage/mem_only_storage.h"

using namespace std;
using namespace slog;

TEST(ServerTest, LookupMaster) {
  const string REQUESTER_CHANNEL("requester");
  ConfigVec configs = MakeTestConfigurations("lookup", 1, 1);
  TestSlog test_slog(configs[0]);
  test_slog
    .WithServerAndClient()
    .Data("A", {"vzxcv", 0, 1})
    .Data("B", {"fbczx", 1, 1})
    .Data("C", {"bzxcv", 2, 2});

  unique_ptr<Channel> requester(
      test_slog.AddChannel(REQUESTER_CHANNEL));

  test_slog.StartInNewThreads();

  internal::Request req;
  auto lookup = req.mutable_lookup_master();
  lookup->set_txn_id(1234);
  lookup->add_keys("A");
  lookup->add_keys("B");
  lookup->add_keys("D");
  MMessage msg;
  msg.Set(MM_PROTO, req);
  msg.Set(MM_FROM_CHANNEL, REQUESTER_CHANNEL);
  msg.Set(MM_TO_CHANNEL, SERVER_CHANNEL);
  requester->Send(msg);

  requester->Receive(msg);
  internal::Response res;
  ASSERT_TRUE(msg.GetProto(res));
  ASSERT_TRUE(res.has_lookup_master());
  auto lookup_result = res.lookup_master().master_metadata();
  ASSERT_EQ(0, lookup_result["A"].master());
  ASSERT_EQ(1, lookup_result["A"].counter());
  ASSERT_EQ(1, lookup_result["B"].master());
  ASSERT_EQ(1, lookup_result["B"].counter());
  ASSERT_EQ(0, lookup_result.count("D"));
}