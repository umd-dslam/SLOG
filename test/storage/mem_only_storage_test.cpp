#include <gtest/gtest.h>

#include "storage/mem_only_storage.h"

TEST(MemOnlyStorageTest, ReadWriteTest) {
  MemOnlyStorage storage;
  Key key = "key1";
  Key value = "value1";
  Record record(value, 0);
  storage.Write(key, record);

  Record ret;
  bool ok = storage.Read(key, &ret);
  ASSERT_TRUE(ok);
  ASSERT_EQ(value, ret.value);
}