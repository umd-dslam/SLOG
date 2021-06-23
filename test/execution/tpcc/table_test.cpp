#include "execution/tpcc/table.h"

#include <iostream>
#include <gtest/gtest.h>

using namespace std;
using namespace slog::tpcc;

void PrintScalarList(const std::vector<ScalarPtr>& sl) {
  std::cout << "(";
  bool first = true;
  for (const auto& s : sl) {
    if (!first) {
      std::cout << ", ";
    }
    std::cout << s->to_string();
    first = false;
  }
  std::cout << ")";
}

bool ScalarListsEqual(const std::vector<ScalarPtr>& s1, const std::vector<ScalarPtr>& s2) {
  bool ok = true;
  if (s1.size() != s2.size()) {
    ok = false;
  } else {
    for (size_t i = 0; i < s1.size(); i++) {
      if (!(*s1[i] == *s2[i])) {
        ok = false;
        break;
      }
    }
  }
  if (!ok) {
    PrintScalarList(s1);
    std::cout << "\n";
    PrintScalarList(s2);
    std::cout << "\n";
  }
  return ok;
}

TEST(TableTest, Insert) {
  auto kv_table = make_shared<KeyValueTable>();
  auto deleted_keys = make_shared<KeyList>();
  Table<WarehouseSchema> table(kv_table, deleted_keys);

  std::vector<std::vector<ScalarPtr>> data = {
    {
      MakeScalar(Int32Type::Get(), 10),
      MakeScalar(FixedTextType<10>::Get(), "Maryland--"),
      MakeScalar(FixedTextType<20>::Get(), "Baltimore Blvd.-----"),
      MakeScalar(FixedTextType<20>::Get(), "Paint Branch Drive--"),
      MakeScalar(FixedTextType<20>::Get(), "College Park--------"),
      MakeScalar(FixedTextType<2>::Get(), "MA"),
      MakeScalar(FixedTextType<9>::Get(), "20742----"),
      MakeScalar(Int32Type::Get(), 1234),
      MakeScalar(Int32Type::Get(), 1234567)
    },
    {
      MakeScalar(Int32Type::Get(), 20),
      MakeScalar(FixedTextType<10>::Get(), "Goods Inc."),
      MakeScalar(FixedTextType<20>::Get(), "Main Street---------"),
      MakeScalar(FixedTextType<20>::Get(), "Main Street 2-------"),
      MakeScalar(FixedTextType<20>::Get(), "Some City-----------"),
      MakeScalar(FixedTextType<2>::Get(), "CA"),
      MakeScalar(FixedTextType<9>::Get(), "12345----"),
      MakeScalar(Int32Type::Get(), 4214),
      MakeScalar(Int32Type::Get(), 521232)
    }
  };

  table.Insert(data[0]);
  table.Insert(data[1]);

  {
    std::vector<ScalarPtr> row = {MakeScalar(Int32Type::Get(), 10)};
    auto res = table.Select(row);
    row.insert(row.end(), res.begin(), res.end());
    ASSERT_TRUE(ScalarListsEqual(row, data[0]));
  }
  {
    std::vector<ScalarPtr> row = {MakeScalar(Int32Type::Get(), 20)};
    auto res = table.Select(row);
    row.insert(row.end(), res.begin(), res.end());
    ASSERT_TRUE(ScalarListsEqual(row, data[1]));
  }
}
