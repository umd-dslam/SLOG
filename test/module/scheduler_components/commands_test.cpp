#include "module/scheduler_components/commands.h"

#include <gtest/gtest.h>

#include "common/proto_utils.h"
#include "test/test_utils.h"
#include "storage/mem_only_storage.h"

using namespace std;
using namespace slog;

TEST(CommandsTest, SimpleKeyValueProcedures) {
  auto config = MakeTestConfigurations("", 1, 1);
  auto storage = std::make_shared<MemOnlyStorage<Key, Record, Metadata>>();
  auto txn = MakeTransaction(
      {{"key1", KeyType::READ}, {"key2", KeyType::WRITE}, {"key3", KeyType::WRITE}, {"key4", KeyType::WRITE}},
      "GET  key1\n"
      "SET  key2 value2\n"
      "DEL  key4\n"
      "COPY key1 key3\n");
  txn->mutable_keys()->at("key1").set_value("value1");

  KeyValueCommands<Key, Record> proc(config[0], storage);
  proc.Execute(*txn);
  ASSERT_EQ(txn->status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(txn->keys_size(), 4);
  ASSERT_EQ(txn->keys().at("key1").type(), KeyType::READ);
  ASSERT_EQ(txn->keys().at("key1").value(), "value1");
  ASSERT_EQ(txn->keys().at("key2").type(), KeyType::WRITE);
  ASSERT_EQ(txn->keys().at("key2").new_value(), "value2");
  ASSERT_EQ(txn->keys().at("key3").type(), KeyType::WRITE);
  ASSERT_EQ(txn->keys().at("key3").new_value(), "value1");
  ASSERT_EQ(txn->deleted_keys_size(), 1);
  ASSERT_EQ(txn->deleted_keys(0), "key4");
}

TEST(CommandsTest, KeyValueAbortedNotEnoughArgs) {
  auto config = MakeTestConfigurations("", 1, 1);
  auto storage = std::make_shared<MemOnlyStorage<Key, Record, Metadata>>();
  auto txn = MakeTransaction({{"key1", KeyType::WRITE}}, "SET key1");

  KeyValueCommands<Key, Record> proc(config[0], storage);
  proc.Execute(*txn);
  ASSERT_EQ(txn->status(), TransactionStatus::ABORTED);
}

TEST(CommandsTest, KeyValueAbortedInvalidCommand) {
  auto config = MakeTestConfigurations("", 1, 1);
  auto storage = std::make_shared<MemOnlyStorage<Key, Record, Metadata>>();
  auto txn = MakeTransaction({{"key1"}}, "WRONG");

  KeyValueCommands<Key, Record> proc(config[0], storage);
  proc.Execute(*txn);
  ASSERT_EQ(txn->status(), TransactionStatus::ABORTED);
}

TEST(CommandsTest, KeyValueOnlyWritesKeysInWriteSet) {
  auto config = MakeTestConfigurations("", 1, 1);
  auto storage = std::make_shared<MemOnlyStorage<Key, Record, Metadata>>();
  auto txn = MakeTransaction({{"key1"}, {"key2", KeyType::WRITE}, {"key3", KeyType::WRITE}},
                             "GET key1\n"
                             "SET key2 value2\n"
                             "SET key4 value4\n"
                             "DEL key3");

  KeyValueCommands<Key, Record> proc(config[0], storage);
  proc.Execute(*txn);
  ASSERT_EQ(txn->status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(txn->keys_size(), 3);
  ASSERT_EQ(txn->keys().at("key2").new_value(), "value2");
  ASSERT_EQ(txn->deleted_keys_size(), 1);
  ASSERT_EQ(txn->deleted_keys(0), "key3");
}