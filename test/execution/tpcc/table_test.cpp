#include "execution/tpcc/table.h"

#include <gtest/gtest.h>

#include <iostream>

#include "common/proto_utils.h"
#include "execution/tpcc/metadata_initializer.h"
#include "storage/mem_only_storage.h"

using namespace std;
using namespace slog;
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
    // clang-format off
    data = {{MakeInt32Scalar(1000),
             MakeInt8Scalar(2),
             MakeFixedTextScalar<10>("UMD-------"),
             MakeFixedTextScalar<20>("Baltimore Blvd.-----"),
             MakeFixedTextScalar<20>("Paint Branch Drive--"),
             MakeFixedTextScalar<20>("College Park--------"),
             MakeFixedTextScalar<2>("MA"),
             MakeFixedTextScalar<9>("20742----"),
             MakeInt32Scalar(1234),
             MakeInt64Scalar(1234567),
             MakeInt32Scalar(4321)},
            {MakeInt32Scalar(2001),
             MakeInt8Scalar(3),
             MakeFixedTextScalar<10>("Goods Inc."),
             MakeFixedTextScalar<20>("Main Street---------"),
             MakeFixedTextScalar<20>("Main Street 2-------"),
             MakeFixedTextScalar<20>("Some City-----------"),
             MakeFixedTextScalar<2>("CA"),
             MakeFixedTextScalar<9>("12345----"),
             MakeInt32Scalar(4214),
             MakeInt64Scalar(521232),
             MakeInt32Scalar(6476)}};
    // clang-format on

    storage = std::make_shared<MemOnlyStorage>();
    auto metadata_initializer = std::make_shared<TPCCMetadataInitializer>(2, 1);
    auto storage_adapter = std::make_shared<StorageInitializingAdapter>(storage, metadata_initializer);
    Table<DistrictSchema> storage_table(storage_adapter);
    for (const auto& row : data) {
      // Populate data to the storage
      storage_table.Insert(row);
      // Populate keys to the txn
      auto new_entry = txn.mutable_keys()->Add();
      new_entry->set_key(Table<DistrictSchema>::MakeStorageKey({row.begin(), row.begin() + 2}));
      new_entry->mutable_value_entry()->set_type(KeyType::WRITE);
      // This is to check if the metadata initializer is working correctly
      auto home = std::static_pointer_cast<Int32Scalar>(row[0])->value % 2;
      new_entry->mutable_value_entry()->mutable_metadata()->set_master(home);
      new_entry->mutable_value_entry()->mutable_metadata()->set_counter(0);
    }

    auto txn_adapter = std::make_shared<TxnStorageAdapter>(txn);
    txn_table = std::make_unique<Table<DistrictSchema>>(txn_adapter);

    // Populate initial values to the txn
    FlushAndRefreshTxn();
  }

  void FlushAndRefreshTxn() {
    for (const auto& kv : txn.keys()) {
      const auto& key = kv.key();
      const auto& value = kv.value_entry();
      if (!value.new_value().empty()) {
        Record record(value.new_value());
        record.SetMetadata(value.metadata());
        storage->Write(key, record);
      }
    }
    for (const auto& key : txn.deleted_keys()) {
      storage->Delete(key);
    }
    for (auto& kv : *(txn.mutable_keys())) {
      const auto& key = kv.key();
      Record record;
      ASSERT_TRUE(storage->Read(key, record));
      auto value = kv.mutable_value_entry();
      ASSERT_EQ(value->metadata().master(), record.metadata().master);
      value->set_value(record.to_string());
      value->clear_new_value();
    }
    txn.clear_deleted_keys();
  }

  std::vector<std::vector<ScalarPtr>> data;
  std::shared_ptr<Storage> storage;
  Transaction txn;
  std::unique_ptr<Table<DistrictSchema>> txn_table;
};

TEST_F(TableTest, InsertAndSelect) {
  for (const auto& row : data) {
    std::vector<ScalarPtr> pkey{row.begin(), row.begin() + 2};
    ASSERT_TRUE(ScalarListsEqual(txn_table->Select(pkey), row));
  }
  ASSERT_EQ(txn.keys_size(), data.size());
  ASSERT_EQ(txn.deleted_keys_size(), 0);
}

TEST_F(TableTest, UpdateAndSelect) {
  auto new_name = MakeFixedTextScalar<10>("AAAAAAAAAA");
  auto new_ytd = MakeInt64Scalar(9876543210);
  for (const auto& row : data) {
    std::vector<ScalarPtr> pkey{row.begin(), row.begin() + 2};
    ASSERT_TRUE(
        txn_table->Update(pkey, {DistrictSchema::Column::NAME, DistrictSchema::Column::YTD}, {new_name, new_ytd}));
  }
  ASSERT_EQ(txn.keys_size(), data.size());
  ASSERT_EQ(txn.deleted_keys_size(), 0);

  FlushAndRefreshTxn();

  for (const auto& row : data) {
    std::vector<ScalarPtr> pkey{row.begin(), row.begin() + DistrictSchema::PKeySize};

    auto res = txn_table->Select(pkey, {DistrictSchema::Column::ID, DistrictSchema::Column::YTD,
                                        DistrictSchema::Column::NAME, DistrictSchema::Column::STREET_1});
    ASSERT_TRUE(ScalarListsEqual(res, {row[1], new_ytd, new_name, row[3]}));
  }
}

TEST_F(TableTest, Delete) {
  ASSERT_TRUE(txn_table->Delete({data[0].begin(), data[0].begin() + DistrictSchema::PKeySize}));
  ASSERT_FALSE(txn_table->Delete({data[0].begin(), data[0].begin() + DistrictSchema::PKeySize}));
  ASSERT_EQ(txn.keys_size(), data.size() - 1);
  ASSERT_EQ(txn.deleted_keys_size(), 1);

  FlushAndRefreshTxn();

  for (size_t i = 1; i < data.size(); i++) {
    std::vector<ScalarPtr> pkey{data[i].begin(), data[i].begin() + DistrictSchema::PKeySize};
    auto res = txn_table->Select(pkey);
    ASSERT_TRUE(ScalarListsEqual(res, data[i]));
  }
  ASSERT_TRUE(txn_table->Select({data[0].begin(), data[0].begin() + DistrictSchema::PKeySize}).empty());  
}