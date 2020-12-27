#include "common/string_utils.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <iostream>

using namespace std;
using namespace slog;
using namespace testing;

TEST(StringUtilsTest, NextTokenTest) {
  string res;

  {
    string str = "hello world, c++";
    string delims = " ,";
    auto pos = NextToken(res, str, delims);
    ASSERT_EQ(res, "hello");
    ASSERT_EQ(pos, 5U);

    pos = NextToken(res, str, delims, pos);
    ASSERT_EQ(res, "world");
    ASSERT_EQ(pos, 11U);

    pos = NextToken(res, str, delims, pos);
    ASSERT_EQ(res, "c++");
    ASSERT_EQ(pos, 16U);

    pos = NextToken(res, str, delims, pos);
    ASSERT_EQ(res, "");
    ASSERT_EQ(pos, string::npos);
  }

  {
    string str = "  hello  \t\t world !!! ";
    string delims = " \t";
    auto pos = NextToken(res, str, delims);
    ASSERT_EQ(res, "hello");
    ASSERT_EQ(pos, 7U);

    pos = NextToken(res, str, delims, pos);
    ASSERT_EQ(res, "world");
    ASSERT_EQ(pos, 17U);

    pos = NextToken(res, str, delims, pos);
    ASSERT_EQ(res, "!!!");
    ASSERT_EQ(pos, 21U);

    pos = NextToken(res, str, delims, pos);
    ASSERT_EQ(res, "");
    ASSERT_EQ(pos, string::npos);
  }
}

TEST(StringUtilsTest, NextNTokensTest) {
  vector<string> res;

  {
    string str = "this is   a   test  ";
    string delims = " ";
    auto pos = NextNTokens(res, str, delims, 3);
    ASSERT_THAT(res, ElementsAre("this", "is", "a"));
    ASSERT_EQ(pos, 11U);

    pos = NextNTokens(res, str, delims, 1, pos);
    ASSERT_THAT(res, ElementsAre("test"));
    ASSERT_EQ(pos, 18U);

    pos = NextNTokens(res, str, delims, 1, pos);
    ASSERT_TRUE(res.empty());
    ASSERT_EQ(pos, string::npos);
  }

  {
    string str = "  one \t two \t three  ";
    string delims = " \t";
    auto pos = NextNTokens(res, str, delims, 4);
    ASSERT_TRUE(res.empty());
    ASSERT_EQ(pos, string::npos);
  }
}

TEST(StringUtilsTest, TrimTest) {
  ASSERT_EQ(Trim("       trim this plz    "), "trim this plz");
  ASSERT_EQ(Trim(" \t\t trim this plz \t\t    "), "trim this plz");
}