#include "data_structure/concurrent_hash_map.h"

#include <gtest/gtest.h>

#include <thread>

using namespace std;
using namespace slog;

TEST(ConcurrentHashMapTest, SerialBasicOperations) {
  ConcurrentHashMap<string, string> map;
  string result;
  ASSERT_FALSE(map.Get(result, "test"));
  ASSERT_FALSE(map.Erase("test"));

  for (size_t i = 0; i < 10; i++) {
    ASSERT_FALSE(map.InsertOrUpdate(to_string(i), "foo"));
  }
  for (size_t i = 0; i < 10; i++) {
    ASSERT_TRUE(map.Get(result, to_string(i)));
    ASSERT_EQ(result, "foo");
  }
  for (size_t i = 0; i < 10; i++) {
    auto val = map.GetUnsafe(to_string(i));
    ASSERT_NE(val, nullptr);
    ASSERT_EQ(*val, "foo");
  }
  ASSERT_TRUE(map.InsertOrUpdate("0", "bar"));
  ASSERT_TRUE(map.Get(result, "0"));
  ASSERT_EQ(result, "bar");

  for (size_t i = 0; i < 10; i++) {
    ASSERT_TRUE(map.Erase(to_string(i)));
  }
  for (size_t i = 0; i < 10; i++) {
    ASSERT_FALSE(map.Get(result, to_string(i)));
    ASSERT_EQ(map.GetUnsafe(to_string(i)), nullptr);
  }
}

TEST(ConcurrentHashMapTest, TriggerRehash) {
  ConcurrentHashMap<string, string> map;
  string result;

  for (size_t i = 0; i < 10000; i++) {
    ASSERT_FALSE(map.InsertOrUpdate(to_string(i), "foo" + to_string(i)));
  }

  for (size_t i = 0; i < 10000; i++) {
    ASSERT_TRUE(map.Get(result, to_string(i))) << "Failed at i = " << i;
    ASSERT_EQ(result, "foo" + to_string(i)) << "Failed at i = " << i;
  }

  for (size_t i = 0; i < 10000; i++) {
    ASSERT_TRUE(map.Erase(to_string(i)));
  }

  for (size_t i = 0; i < 10000; i++) {
    ASSERT_FALSE(map.Get(result, to_string(i)));
  }
}

TEST(ConcurrentHashMapTest, TwoReadersOneWriter) {
  uint32_t N = 500000;
  string key = "foo";
  ConcurrentHashMap<string, string> map;

  auto Updates = [&]() {
    for (size_t i = 0; i < N; i++) {
      map.InsertOrUpdate(key, to_string(i));
    }
  };

  auto Gets = [&]() {
    int prev = 0;
    string result;
    for (size_t i = 0; i < N; i++) {
      if (map.Get(result, key)) {
        auto x = stoi(result);
        ASSERT_GE(x, prev);
        prev = x;
      }
    }
  };

  thread w(Updates);
  thread r1(Gets);
  thread r2(Gets);
  w.join();
  r1.join();
  r2.join();
}

TEST(ConcurrentHashMapTest, TwoWritersDifferentKeys) {
  int N = 500000;
  ConcurrentHashMap<string, string> map;

  auto Updates = [&](int start) {
    for (int i = start; i < N; i += 2) {
      map.InsertOrUpdate(to_string(i), to_string(i));
    }
  };

  thread w1(Updates, 0);
  thread w2(Updates, 1);
  w1.join();
  w2.join();

  string result;
  for (int i = 0; i < N; i++) {
    ASSERT_TRUE(map.Get(result, to_string(i)));
    ASSERT_EQ(result, to_string(i));
  }
}

TEST(ConcurrentHashMapTest, TwoWritersSameKey) {
  int N = 500000;
  string key = "foo";
  ConcurrentHashMap<string, string> map;

  auto Updates = [&]() {
    for (int i = 0; i < N; i += 1) {
      map.InsertOrUpdate(key, to_string(i));
    }
  };

  thread w1(Updates);
  thread w2(Updates);
  w1.join();
  w2.join();

  string result;
  ASSERT_TRUE(map.Get(result, key));
  ASSERT_EQ(result, to_string(N - 1));
}

TEST(ConcurrentHashMapTest, OneWriterOneEraser) {
  int N = 100000;
  ConcurrentHashMap<string, string> map;

  auto Updates = [&]() {
    for (int i = 0; i < N; i++) {
      map.InsertOrUpdate(to_string(i), to_string(i));
    }
  };

  auto Erases = [&]() {
    for (int i = 0; i < N; i++) {
      map.Erase(to_string(i));
    }
  };

  thread w1(Updates);
  thread w2(Erases);
  w1.join();
  w2.join();

  string result;
  for (int i = 0; i < N; i++) {
    if (map.Get(result, to_string(i))) {
      ASSERT_EQ(result, to_string(i));
    }
  }
}