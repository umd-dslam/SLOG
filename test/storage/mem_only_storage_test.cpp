#include "storage/mem_only_storage.h"

#include <gtest/gtest.h>

#include "common/types.h"

using namespace slog;

TEST(MemOnlyStorageTest, ReadWriteTest) {
  MemOnlyStorage storage;
  Key key = "key1";
  Value value = "value1";
  Record record(value, 0);
  storage.Write(key, record);

  Record ret;
  bool ok = storage.Read(key, ret);
  ASSERT_TRUE(ok);
  ASSERT_EQ(value, ret.to_string());
}