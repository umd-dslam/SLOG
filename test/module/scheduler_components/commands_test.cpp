#include <gtest/gtest.h>

#include "common/proto_utils.h"
#include "module/scheduler_components/commands.h"

using namespace std;
using namespace slog;

TEST(CommandsTest, SimpleKeyValueProcedures) {
  auto txn = MakeTransaction(
      {"key1"},
      {"key2", "key3", "key4"},
      "GET key1\n"
      "SET key2 value2\n"
      "DEL key4\n"
      "COPY key1 key3\n");
  (*(txn->mutable_read_set()))["key1"] = "value1";

  KeyValueCommands proc;
  proc.Execute(*txn);
  ASSERT_EQ(txn->status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(txn->read_set_size(), 1);
  ASSERT_EQ(txn->read_set().at("key1"), "value1");
  ASSERT_EQ(txn->write_set_size(), 3);
  ASSERT_EQ(txn->write_set().at("key2"), "value2");
  ASSERT_EQ(txn->write_set().at("key3"), "value1");
  ASSERT_EQ(txn->delete_set_size(), 1);
  ASSERT_EQ(txn->delete_set(0), "key4");
}

TEST(CommandsTest, KeyValueAbortedNotEnoughArgs) {
  auto txn = MakeTransaction(
      {},
      {"key1", "key2", "key3"},
      "SET key1");

  KeyValueCommands proc;
  proc.Execute(*txn);
  ASSERT_EQ(txn->status(), TransactionStatus::ABORTED);
}

TEST(CommandsTest, KeyValueAbortedInvalidCommand) {
  auto txn = MakeTransaction(
      {},
      {"key1", "key2", "key3"},
      "WRONG");

  KeyValueCommands proc;
  proc.Execute(*txn);
  ASSERT_EQ(txn->status(), TransactionStatus::ABORTED);
}

TEST(CommandsTest, KeyValueOnlyWritesKeysInWriteSet) {
  auto txn = MakeTransaction(
      {"key1"},
      {"key2", "key3"},
      "GET key1\n"
      "SET key2 value2\n"
      "SET key4 value4\n"
      "DEL key3");

  KeyValueCommands proc;
  proc.Execute(*txn);
  ASSERT_EQ(txn->status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(txn->write_set_size(), 2);
  ASSERT_EQ(txn->write_set().at("key2"), "value2");
  ASSERT_EQ(txn->delete_set_size(), 1);
  ASSERT_EQ(txn->delete_set(0), "key3");
}