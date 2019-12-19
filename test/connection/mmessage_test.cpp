#include <gtest/gtest.h>

#include <iostream>

#include "connection/mmessage.h"

using namespace std;
using namespace slog;

class MMessageTest : public ::testing::Test {
protected:
  void SetUp() override {
    proto::Request request;
    auto txn_request = request.mutable_transaction();
    auto txn = txn_request->mutable_transaction();
    txn->add_read_set("A");
    txn->add_read_set("B");
    txn->add_write_set("C");
    message_.FromRequest(request);
  }

  void AssertRequest(const proto::Request& request) {
    ASSERT_EQ(proto::Request::TypeCase::kTransaction, message_.GetType());

    ASSERT_TRUE(request.has_transaction());
    auto re_txn = request.transaction().transaction();
    ASSERT_EQ(2, re_txn.read_set_size());
    ASSERT_EQ(1, re_txn.write_set_size());
  }

  MMessage message_;
};

TEST_F(MMessageTest, FromAndToRequestProto) {
  proto::Request re_request;
  ASSERT_TRUE(message_.ToRequest(re_request));
  AssertRequest(re_request);
}

TEST_F(MMessageTest, SendAndReceive) {
  zmq::context_t context(1);

  zmq::socket_t router(context, ZMQ_ROUTER);
  router.bind("ipc://test.ipc");
  zmq::socket_t dealer(context, ZMQ_DEALER);
  dealer.connect("ipc://test.ipc");

  message_.Send(dealer);
  MMessage recv_msg(router);
  proto::Request request;
  recv_msg.ToRequest(request);
  AssertRequest(request);
}

// TEST(MMessageTest, MMessageFromAndToResponseProto) {
//   proto::Response response;
//   auto txn_response = response.mutable_transaction();
//   auto txn = txn_response->mutable_transaction();
//   txn->add_read_set("A");
//   txn->add_read_set("B");
//   txn->add_write_set("C");

//   MMessage message(response);

//   ASSERT_EQ(proto::Response::TypeCase::kTransaction, message.GetType());

//   proto::Response re_response;
//   ASSERT_TRUE(message.ToResponse(re_response));
//   ASSERT_TRUE(re_response.has_transaction());

//   auto re_txn = re_response.transaction().transaction();
//   ASSERT_EQ(2, re_txn.read_set_size());
//   ASSERT_EQ(1, re_txn.write_set_size());
// }