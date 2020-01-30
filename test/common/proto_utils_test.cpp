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
  ASSERT_EQ(mid.replica(), 0);
  ASSERT_EQ(mid.partition(), 0);

  mid = MakeMachineId("12:34");
  ASSERT_EQ(mid.replica(), 12);
  ASSERT_EQ(mid.partition(), 34);

  mid = MakeMachineId("5aa:6bb");
  ASSERT_EQ(mid.replica(), 5);
  ASSERT_EQ(mid.partition(), 6);

  ASSERT_THROW(MakeMachineId("01234"), std::invalid_argument);
  ASSERT_THROW(MakeMachineId("ab12:cd34"), std::invalid_argument);
}