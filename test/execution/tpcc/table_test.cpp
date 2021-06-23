#include "execution/tpcc/table.h"

#include <gtest/gtest.h>

#include <iostream>

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

class TableTest : public ::testing::Test {
 protected:
  void SetUp() {
    warehouse_data = {{MakeInt32Scalar(10), MakeFixedTextScalar<10>("Maryland--"),
                       MakeFixedTextScalar<20>("Baltimore Blvd.-----"), MakeFixedTextScalar<20>("Paint Branch Drive--"),
                       MakeFixedTextScalar<20>("College Park--------"), MakeFixedTextScalar<2>("MA"),
                       MakeFixedTextScalar<9>("20742----"), MakeInt32Scalar(1234), MakeInt64Scalar(1234567)},
                      {MakeInt32Scalar(20), MakeFixedTextScalar<10>("Goods Inc."),
                       MakeFixedTextScalar<20>("Main Street---------"), MakeFixedTextScalar<20>("Main Street 2-------"),
                       MakeFixedTextScalar<20>("Some City-----------"), MakeFixedTextScalar<2>("CA"),
                       MakeFixedTextScalar<9>("12345----"), MakeInt32Scalar(4214), MakeInt64Scalar(521232)}};

    warehouse_table = std::make_unique<Table<WarehouseSchema>>(kv_table, deleted_keys);
    warehouse_table->Insert(warehouse_data[0]);
    warehouse_table->Insert(warehouse_data[1]);
  }

  std::shared_ptr<KeyValueTable> kv_table = make_shared<KeyValueTable>();
  std::shared_ptr<KeyList> deleted_keys = make_shared<KeyList>();

  std::vector<std::vector<ScalarPtr>> warehouse_data;
  std::unique_ptr<Table<WarehouseSchema>> warehouse_table;
};

TEST_F(TableTest, InsertAndSelect) {
  ASSERT_TRUE(ScalarListsEqual(warehouse_table->Select({MakeInt32Scalar(10)}), warehouse_data[0]));
  ASSERT_TRUE(ScalarListsEqual(warehouse_table->Select({MakeInt32Scalar(20)}), warehouse_data[1]));
  ASSERT_EQ(kv_table->size(), 2);
  ASSERT_TRUE(deleted_keys->empty());
}

TEST_F(TableTest, UpdateAndSelect) {
  std::vector<ScalarPtr> pkey = {MakeInt32Scalar(10)};
  auto new_value = MakeFixedTextScalar<10>("Virginia--");
  ASSERT_TRUE(warehouse_table->Update(pkey, {WarehouseSchema::Column::NAME}, {new_value}));
  auto res = warehouse_table->Select(pkey, {WarehouseSchema::Column::ID, WarehouseSchema::Column::NAME});
  ASSERT_TRUE(ScalarListsEqual(res, {MakeInt32Scalar(10), MakeFixedTextScalar<10>("Virginia--")}));
  ASSERT_EQ(kv_table->size(), 2);
  ASSERT_TRUE(deleted_keys->empty());
}
