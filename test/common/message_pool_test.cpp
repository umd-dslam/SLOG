#include <gtest/gtest.h>

#include "common/message_pool.h"

using namespace std;
using namespace slog;

TEST(MessagePoolTest, AcquireAndReturn) {
  MessagePool<int> pool(2);
  ASSERT_EQ(pool.size(), 2);
  auto msg1 = pool.Acquire();
  ASSERT_EQ(pool.size(), 1);
  auto msg2 = pool.Acquire();
  ASSERT_EQ(pool.size(), 0);
  auto msg3 = pool.Acquire();
  ASSERT_EQ(pool.size(), 0);
  pool.Return(msg1);
  ASSERT_EQ(pool.size(), 1);
  pool.Return(msg2);
  ASSERT_EQ(pool.size(), 2);
  pool.Return(msg3);
  ASSERT_EQ(pool.size(), 3);
}

TEST(ReusableMessageTest, CreateAndDestroy) {
  MessagePool<int> pool(2);
  {
    ReusableMessage<int> msg(&pool);
    ASSERT_EQ(pool.size(), 1);
  }
  ASSERT_EQ(pool.size(), 2);
}

TEST(ReusableMessageTest, Copy) {
  MessagePool<int> pool(3);
  ReusableMessage<int> msg1(&pool);
  *msg1.get() = 1234;
  ASSERT_EQ(pool.size(), 2);

  ReusableMessage<int> msg2(msg1);
  ASSERT_NE(msg2.get(), msg1.get());
  ASSERT_EQ(*msg2.get(), 1234);
  ASSERT_EQ(pool.size(), 1);

  ReusableMessage<int> msg3 = msg2;
  ASSERT_NE(msg3.get(), msg2.get());
  ASSERT_EQ(*msg3.get(), 1234);
  ASSERT_EQ(pool.size(), 0);
}

TEST(ReusableMessageTest, Move) {
  MessagePool<int> pool(3);
  ReusableMessage<int> msg1(&pool);
  *msg1.get() = 1234;
  ASSERT_EQ(pool.size(), 2);

  ReusableMessage<int> msg2(move(msg1));
  ASSERT_EQ(*msg2.get(), 1234);
  ASSERT_EQ(pool.size(), 2);

  ReusableMessage<int> msg3 = move(msg2);
  ASSERT_EQ(*msg3.get(), 1234);
  ASSERT_EQ(pool.size(), 2);
}
