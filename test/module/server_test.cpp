#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/proto_utils.h"
#include "common/test_utils.h"
#include "connection/broker.h"
#include "proto/internal.pb.h"

#include "connection/broker.h"
#include "module/server.h"
#include "storage/mem_only_storage.h"

using namespace std;
using namespace slog;

TEST(ServerTest, LookupMaster) {
  ConfigVec configs = MakeTestConfigurations("lookup", 1, 1);
  auto context = std::make_shared<zmq::context_t>(1);
  auto storage = std::make_shared<MemOnlyStorage>();
  storage->Write("A", Record("vzxcv", 0, 1));
  storage->Write("B", Record("fbczx", 1, 1));
  storage->Write("C", Record("bzxcv", 2, 2));

  Broker broker(configs[0], context);
  ModuleRunner server(new Server(configs[0], context, broker, storage));

  std::unique_ptr<Channel> client(broker.AddChannel("client"));

  broker.StartInNewThread();
  server.StartInNewThread();

  internal::Request req;
  auto lookup = req.mutable_lookup_master();
  lookup->set_txn_id(1234);
  lookup->add_keys("A");
  lookup->add_keys("B");
  lookup->add_keys("D");
  MMessage msg;
  msg.Set(MM_REQUEST, req);
  msg.Set(MM_FROM_CHANNEL, "client");
  msg.Set(MM_TO_CHANNEL, "server");
  client->Send(msg);

  client->Receive(msg);
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