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
  void SetUp(string&& prefix, uint32_t num_replicas, uint32_t num_partitions) {
    storage_ = make_shared<MemOnlyStorage>();
    storage_->Write("A", {"vzxcv", 0, 1});
    storage_->Write("B", {"fbczx", 0, 2});
    storage_->Write("C", {"bzxcv", 1, 2});
    storage_->Write("D", {"naeqw", 2, 1});

    ConfigVec configs = MakeTestConfigurations(
        move(prefix), num_replicas, num_partitions);
    auto server_context = make_shared<zmq::context_t>(1);
    broker_ = make_unique<Broker>(configs[0], server_context);
    server_ = MakeRunnerFor<Server>(configs[0], *server_context, *broker_, storage_);
    forwarder_ = MakeRunnerFor<Forwarder>(configs[0], *broker_);

    sink_.reset(broker_->AddChannel(SEQUENCER_CHANNEL));

    broker_->StartInNewThread();
    server_->StartInNewThread();
    forwarder_->StartInNewThread();

    client_context_ = make_unique<zmq::context_t>(1);
    client_socket_ = make_unique<zmq::socket_t>(*client_context_, ZMQ_DEALER);
    string endpoint = "tcp://localhost:" + to_string(configs[0]->GetServerPort());
    client_socket_->connect(endpoint);
  }

  void SendTxn(const Transaction& txn) {
    api::Request request;
    auto txn_req = request.mutable_txn();
    txn_req->mutable_txn()->CopyFrom(txn);
    MMessage msg;
    msg.Push(request);
    msg.SendTo(*client_socket_);
  }

  void Receive(MMessage& msg) {
    sink_->Receive(msg);
  }

  unique_ptr<zmq::context_t> client_context_;
  shared_ptr<MemOnlyStorage> storage_;
  unique_ptr<Broker> broker_;
  unique_ptr<ModuleRunner> server_;
  unique_ptr<ModuleRunner> forwarder_;
  unique_ptr<zmq::socket_t> client_socket_;
  unique_ptr<Channel> sink_;
};

TEST_F(ForwarderTest, SingleRegionSinglePartition) {
  SetUp("single_forwarder", 1, 1);
  auto txn = MakeTransaction({"A"}, {{"B", "data"}});
  SendTxn(txn);

  MMessage msg;
  Receive(msg);
  internal::Request req;
  ASSERT_TRUE(msg.GetProto(req));
  ASSERT_TRUE(req.has_forward());
  const auto& forwarded_txn = req.forward().txn();
  ASSERT_EQ(
      TransactionType::SINGLE_HOME, forwarded_txn.internal().type());
  const auto& master_metadata =
      forwarded_txn.internal().master_metadata();
  ASSERT_EQ(0, master_metadata.at("A").master());
  ASSERT_EQ(0, master_metadata.at("B").master());
}

TEST_F(ForwarderTest, SingleRegionSinglePartitionKnownMaster) {
  SetUp("known_master", 1, 1);
  auto txn = MakeTransaction(
      {"A"},           /* read_set*/
      {{"B", "data"}}, /* write set */
      "",              /* code */
      {{"A", {0, 0}},  /* master_metadata */
       {"B", {0, 0}}});
  SendTxn(txn);

  MMessage msg;
  Receive(msg);
  internal::Request req;
  ASSERT_TRUE(msg.GetProto(req));
  ASSERT_TRUE(req.has_forward());
  const auto& forwarded_txn = req.forward().txn();
  ASSERT_EQ(
      TransactionType::SINGLE_HOME, forwarded_txn.internal().type());
  const auto& master_metadata =
      forwarded_txn.internal().master_metadata();
  ASSERT_EQ(0, master_metadata.at("A").master());
  ASSERT_EQ(0, master_metadata.at("B").master());
}