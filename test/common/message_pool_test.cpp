#include "common/message_pool.h"

#include <gtest/gtest.h>

#include "proto/internal.pb.h"

using namespace std;
using namespace slog;
using namespace slog::internal;

TEST(MessagePoolTest, AcquireAndReturn) {
  MessagePool<Request> pool(2);
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
  MessagePool<Request> pool(2);
  {
    ReusableMessage<Request> msg(&pool);
    ASSERT_EQ(pool.size(), 1);
  }
  ASSERT_EQ(pool.size(), 2);
}

TEST(ReusableMessageTest, Copy) {
  MessagePool<Request> pool(3);
  ReusableMessage<Request> msg1(&pool);
  msg1.get()->mutable_echo()->set_data("abc");
  ASSERT_EQ(pool.size(), 2);

  ReusableMessage<Request> msg2(msg1);
  ASSERT_NE(msg2.get(), msg1.get());
  ASSERT_EQ(msg2.get()->echo().data(), "abc");
  ASSERT_EQ(pool.size(), 1);

  ReusableMessage<Request> msg3 = msg2;
  ASSERT_NE(msg3.get(), msg2.get());
  ASSERT_EQ(msg3.get()->echo().data(), "abc");
  ASSERT_EQ(pool.size(), 0);
}

TEST(ReusableMessageTest, Move) {
  MessagePool<Request> pool(3);
  ReusableMessage<Request> msg1(&pool);
  msg1.get()->mutable_echo()->set_data("abc");
  ASSERT_EQ(pool.size(), 2);

  ReusableMessage<Request> msg2(move(msg1));
  ASSERT_EQ(msg2.get()->echo().data(), "abc");
  ASSERT_EQ(pool.size(), 2);

  ReusableMessage<Request> msg3 = move(msg2);
  ASSERT_EQ(msg3.get()->echo().data(), "abc");
  ASSERT_EQ(pool.size(), 2);
}
