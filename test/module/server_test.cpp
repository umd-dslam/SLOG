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
  auto configs = MakeTestConfigurations("lookup", 1, 1, 23 /* seed */);
  TestSlog test_slog(configs[0]);
  test_slog.AddServerAndClient();
  test_slog.Data("A", {"vzxcv", 0, 1});
  test_slog.Data("B", {"fbczx", 1, 1});
  test_slog.Data("C", {"bzxcv", 2, 2});

  test_slog.AddOutputChannel(FORWARDER_CHANNEL);
  auto sender = test_slog.GetSender();

  test_slog.StartInNewThreads();

  // Send a lookup request to the server
  internal::Request req;
  auto lookup = req.mutable_lookup_master();
  lookup->set_txn_id(1234);
  lookup->add_keys("A");
  lookup->add_keys("B");
  lookup->add_keys("D");
  sender->Send(req, SERVER_CHANNEL);

  // Wait and receive the response
  MMessage msg;
  test_slog.ReceiveFromOutputChannel(msg, FORWARDER_CHANNEL);
  internal::Response res;
  ASSERT_TRUE(msg.GetProto(res));
  ASSERT_TRUE(res.has_lookup_master());
  auto lookup_result = res.lookup_master().master_metadata();
  ASSERT_EQ(2U, lookup_result.size());
  ASSERT_EQ(0U, lookup_result["A"].master());
  ASSERT_EQ(1U, lookup_result["A"].counter());
  ASSERT_EQ(1U, lookup_result["B"].master());
  ASSERT_EQ(1U, lookup_result["B"].counter());
  ASSERT_EQ(1, res.lookup_master().new_keys_size());
  ASSERT_EQ("D", res.lookup_master().new_keys(0));
}