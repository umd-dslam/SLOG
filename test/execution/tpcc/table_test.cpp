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
      ASSERT_TRUE(storage->Delete(key));
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
};

class UngroupedTableTest : public TableTest {
 protected:
  void SetUp() {
    // clang-format off
    data = {{MakeInt32Scalar(1000),
             MakeInt8Scalar(2),
             MakeFixedTextScalar<10>("UMD-------"),
             MakeFixedTextScalar<71>("Baltimore Blvd.-----Paint Branch Drive--College Park--------MA20742----"),
             MakeInt32Scalar(1234),
             MakeInt64Scalar(1234567),
             MakeInt32Scalar(4321)},
            {MakeInt32Scalar(2001),
             MakeInt8Scalar(3),
             MakeFixedTextScalar<10>("Goods Inc."),
             MakeFixedTextScalar<71>("Main Street---------Main Street 2-------Some City-----------CA12345----"),
             MakeInt32Scalar(4214),
             MakeInt64Scalar(521232),
             MakeInt32Scalar(6476)}};
    // clang-format on

    storage = std::make_shared<MemOnlyStorage>();
    auto metadata_initializer = std::make_shared<TPCCMetadataInitializer>(2, 1);
    auto storage_adapter = std::make_shared<KVStorageAdapter>(storage, metadata_initializer);
    Table<DistrictSchema> storage_table(storage_adapter);
    for (const auto& row : data) {
      // Populate data to the storage
      storage_table.Insert(row);
      // Populate keys to the txn
      auto keys = Table<DistrictSchema>::MakeStorageKeys({row.begin(), row.begin() + DistrictSchema::kPKeySize});
      for (const auto& key : keys) {
        auto new_entry = txn.mutable_keys()->Add();
        new_entry->set_key(key);
        new_entry->mutable_value_entry()->set_type(KeyType::WRITE);
        // This is to check if the metadata initializer is working correctly
        auto home = (std::static_pointer_cast<Int32Scalar>(row[0])->value - 1) % 2;
        new_entry->mutable_value_entry()->mutable_metadata()->set_master(home);
        new_entry->mutable_value_entry()->mutable_metadata()->set_counter(0);
      }
    }

    auto txn_adapter = std::make_shared<TxnStorageAdapter>(txn);
    txn_table = std::make_unique<Table<DistrictSchema>>(txn_adapter);

    // Populate initial values to the txn
    FlushAndRefreshTxn();
  }

  std::unique_ptr<Table<DistrictSchema>> txn_table;
};

TEST_F(UngroupedTableTest, InsertAndSelect) {
  for (const auto& row : data) {
    std::vector<ScalarPtr> pkey{row.begin(), row.begin() + DistrictSchema::kPKeySize};
    ASSERT_TRUE(ScalarListsEqual(txn_table->Select(pkey), row));
  }
  ASSERT_EQ(txn.keys_size(), data.size() * (DistrictSchema::kNonPKeySize));
  ASSERT_EQ(txn.deleted_keys_size(), 0);
}

TEST_F(UngroupedTableTest, UpdateAndSelect) {
  auto new_name = MakeFixedTextScalar<10>("AAAAAAAAAA");
  auto new_ytd = MakeInt64Scalar(9876543210);
  for (const auto& row : data) {
    std::vector<ScalarPtr> pkey{row.begin(), row.begin() + DistrictSchema::kPKeySize};
    ASSERT_TRUE(
        txn_table->Update(pkey, {DistrictSchema::Column::NAME, DistrictSchema::Column::YTD}, {new_name, new_ytd}));
  }
  ASSERT_EQ(txn.keys_size(), data.size() * DistrictSchema::kNonPKeySize);
  ASSERT_EQ(txn.deleted_keys_size(), 0);

  FlushAndRefreshTxn();

  for (const auto& row : data) {
    std::vector<ScalarPtr> pkey{row.begin(), row.begin() + DistrictSchema::kPKeySize};

    auto res = txn_table->Select(pkey, {DistrictSchema::Column::ID, DistrictSchema::Column::YTD,
                                        DistrictSchema::Column::NAME, DistrictSchema::Column::ADDRESS});
    ASSERT_TRUE(ScalarListsEqual(res, {row[1], new_ytd, new_name, row[3]}));
  }
}

TEST_F(UngroupedTableTest, Delete) {
  ASSERT_TRUE(txn_table->Delete({data[0].begin(), data[0].begin() + DistrictSchema::kPKeySize}));
  ASSERT_FALSE(txn_table->Delete({data[0].begin(), data[0].begin() + DistrictSchema::kPKeySize}));
  ASSERT_EQ(txn.keys_size(), (data.size() - 1) * DistrictSchema::kNonPKeySize);
  ASSERT_EQ(txn.deleted_keys_size(), DistrictSchema::kNonPKeySize);

  FlushAndRefreshTxn();

  for (size_t i = 1; i < data.size(); i++) {
    std::vector<ScalarPtr> pkey{data[i].begin(), data[i].begin() + DistrictSchema::kPKeySize};
    auto res = txn_table->Select(pkey);
    ASSERT_TRUE(ScalarListsEqual(res, data[i]));
  }
  ASSERT_TRUE(txn_table->Select({data[0].begin(), data[0].begin() + DistrictSchema::kPKeySize}).empty());
}

class GroupedTableTest : public TableTest {
 protected:
  void SetUp() {
    // clang-format off
    data = {{MakeInt32Scalar(1),
             MakeInt32Scalar(1000),
             MakeInt32Scalar(1000),
             MakeFixedTextScalar<24>("keyboard and mouse------"),
             MakeInt32Scalar(2600),
             MakeFixedTextScalar<50>("something something something something something1")},
            {MakeInt32Scalar(2),
             MakeInt32Scalar(2000),
             MakeInt32Scalar(2000),
             MakeFixedTextScalar<24>("bluetooth headphone-----"),
             MakeInt32Scalar(9900),
             MakeFixedTextScalar<50>("something something something something something2")}};
    // clang-format on

    storage = std::make_shared<MemOnlyStorage>();
    auto metadata_initializer = std::make_shared<TPCCMetadataInitializer>(2, 1);
    auto storage_adapter = std::make_shared<KVStorageAdapter>(storage, metadata_initializer);
    Table<ItemSchema> storage_table(storage_adapter);
    for (const auto& row : data) {
      // Populate data to the storage
      storage_table.Insert(row);
      // Populate keys to the txn
      auto new_entry = txn.mutable_keys()->Add();
      new_entry->set_key(Table<ItemSchema>::MakeStorageKey({row.begin(), row.begin() + ItemSchema::kPKeySize}));
      new_entry->mutable_value_entry()->set_type(KeyType::WRITE);
      // This is to check if the metadata initializer is working correctly
      auto home = (std::static_pointer_cast<Int32Scalar>(row[0])->value - 1) % 2;
      new_entry->mutable_value_entry()->mutable_metadata()->set_master(home);
      new_entry->mutable_value_entry()->mutable_metadata()->set_counter(0);
    }

    auto txn_adapter = std::make_shared<TxnStorageAdapter>(txn);
    txn_table = std::make_unique<Table<ItemSchema>>(txn_adapter);

    // Populate initial values to the txn
    FlushAndRefreshTxn();
  }

  std::unique_ptr<Table<ItemSchema>> txn_table;
};

TEST_F(GroupedTableTest, InsertAndSelect) {
  for (const auto& row : data) {
    std::vector<ScalarPtr> pkey{row.begin(), row.begin() + ItemSchema::kPKeySize};
    ASSERT_TRUE(ScalarListsEqual(txn_table->Select(pkey), row));
  }
  ASSERT_EQ(txn.keys_size(), data.size());
  ASSERT_EQ(txn.deleted_keys_size(), 0);
}

TEST_F(GroupedTableTest, UpdateAndSelect) {
  auto new_name = MakeFixedTextScalar<24>("computer components-----");
  auto new_price = MakeInt32Scalar(1000);
  for (const auto& row : data) {
    std::vector<ScalarPtr> pkey{row.begin(), row.begin() + 2};
    ASSERT_TRUE(txn_table->Update(pkey, {ItemSchema::Column::NAME, ItemSchema::Column::PRICE}, {new_name, new_price}));
  }
  ASSERT_EQ(txn.keys_size(), data.size());
  ASSERT_EQ(txn.deleted_keys_size(), 0);

  FlushAndRefreshTxn();

  for (const auto& row : data) {
    std::vector<ScalarPtr> pkey{row.begin(), row.begin() + ItemSchema::kPKeySize};

    auto res = txn_table->Select(
        pkey, {ItemSchema::Column::ID, ItemSchema::Column::NAME, ItemSchema::Column::PRICE, ItemSchema::Column::DATA});
    ASSERT_TRUE(ScalarListsEqual(res, {row[1], new_name, new_price, row[5]}));
  }
}

TEST_F(GroupedTableTest, Delete) {
  ASSERT_TRUE(txn_table->Delete({data[0].begin(), data[0].begin() + ItemSchema::kPKeySize}));
  ASSERT_FALSE(txn_table->Delete({data[0].begin(), data[0].begin() + ItemSchema::kPKeySize}));
  ASSERT_EQ(txn.keys_size(), data.size() - 1);
  ASSERT_EQ(txn.deleted_keys_size(), 1);

  FlushAndRefreshTxn();

  for (size_t i = 1; i < data.size(); i++) {
    std::vector<ScalarPtr> pkey{data[i].begin(), data[i].begin() + ItemSchema::kPKeySize};
    auto res = txn_table->Select(pkey);
    ASSERT_TRUE(ScalarListsEqual(res, data[i]));
  }
  ASSERT_TRUE(txn_table->Select({data[0].begin(), data[0].begin() + ItemSchema::kPKeySize}).empty());
}