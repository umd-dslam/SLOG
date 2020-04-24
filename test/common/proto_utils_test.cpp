#include <gtest/gtest.h>
#include <iostream>
#include "common/proto_utils.h"

using namespace std;
using namespace slog;

using internal::Request;
using internal::Response;

const string TEST_STRING = "test";

TEST(ProtoUtilsTest, MachineIdStringToMachineId) {
  internal::MachineId mid;

  mid = MakeMachineId("0:0");
  ASSERT_EQ(mid.replica(), 0U);
  ASSERT_EQ(mid.partition(), 0U);

  mid = MakeMachineId("12:34");
  ASSERT_EQ(mid.replica(), 12U);
  ASSERT_EQ(mid.partition(), 34U);

  mid = MakeMachineId("5aa:6bb");
  ASSERT_EQ(mid.replica(), 5U);
  ASSERT_EQ(mid.partition(), 6U);

  ASSERT_THROW(MakeMachineId("01234"), std::invalid_argument);
  ASSERT_THROW(MakeMachineId("ab12:cd34"), std::invalid_argument);
}

TEST(ProtoUtilsTest, ReplicaId) {
  auto txn = MakeTransaction({"A"}, {}, "some code", {{"A", {1, 1}}});
  txn->mutable_internal()->set_id(100);
  TxnReplicaId txn_replica_id = make_pair(100, 1);
  ASSERT_EQ(GetTransactionReplicaId(txn), txn_replica_id);
}
