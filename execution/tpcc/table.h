#pragma once

#include <glog/logging.h>

#include <array>
#include <exception>
#include <iostream>
#include <unordered_map>
#include <vector>

#include "execution/tpcc/scalar.h"
#include "execution/tpcc/storage_adapter.h"

namespace slog {
namespace tpcc {

enum TableId : int8_t { WAREHOUSE, DISTRICT, CUSTOMER, HISTORY, NEW_ORDER, ORDER, ORDER_LINE, ITEM, STOCK };

template <typename Schema>
class Table {
 public:
  using Column = typename Schema::Column;
  static constexpr size_t kNumColumns = Schema::kNumColumns;
  static constexpr size_t kPKeySize = Schema::kPKeySize;

  Table(const StorageAdapterPtr& storage_adapter) : storage_adapter_(storage_adapter) { InitializeColumnOffsets(); }

  std::vector<ScalarPtr> Select(const std::vector<ScalarPtr>& pkey, const std::vector<Column>& columns = {}) {
    std::vector<ScalarPtr> result;
    result.reserve(columns.size());

    auto storage_keys = MakeStorageKeys(pkey, columns);
    bool value_found = false;
    // If no column is provided, select ALL columns
    if (columns.empty()) {
      result.insert(result.end(), pkey.begin(), pkey.end());
      for (size_t i = kPKeySize; i < kNumColumns; i++) {
        auto value = storage_adapter_->Read(storage_keys[i - kPKeySize]);
        if (value != nullptr) {
          result.push_back(MakeScalar(Schema::ColumnTypes[i], reinterpret_cast<const void*>(value->data())));
          value_found = true;
        }
      }
    } else {
      for (size_t i = 0; i < columns.size(); i++) {
        auto col = static_cast<size_t>(columns[i]);
        if (col < kPKeySize) {
          result.push_back(pkey[col]);
        } else {
          auto value = storage_adapter_->Read(storage_keys[i]);
          if (value != nullptr) {
            result.push_back(MakeScalar(Schema::ColumnTypes[col], reinterpret_cast<const void*>(value->data())));
            value_found = true;
          }
        }
      }
    }

    if (!value_found) {
      return {};
    }
    return result;
  }

  bool Update(const std::vector<ScalarPtr>& pkey, const std::vector<Column>& columns,
              const std::vector<ScalarPtr>& values) {
    CHECK_EQ(columns.size(), values.size()) << "Number of values does not match number of columns";

    for (size_t i = 0; i < columns.size(); i++) {
      ValidateType(values[i], columns[i]);
    }

    bool ok = true;
    auto storage_keys = MakeStorageKeys(pkey, columns);
    for (size_t i = 0; i < columns.size(); i++) {
      std::string value(reinterpret_cast<const char*>(values[i]->data()), values[i]->type->size());
      ok &= storage_adapter_->Update(storage_keys[i], std::move(value));
    }
    return ok;
  }

  bool Insert(const std::vector<ScalarPtr>& values) {
    CHECK_EQ(values.size(), kNumColumns) << "Number of values does not match number of columns";

    for (size_t i = kPKeySize; i < kNumColumns; i++) {
      ValidateType(values[i], static_cast<Column>(i));
    }

    bool ok = true;
    auto storage_keys = MakeStorageKeys(values);
    for (size_t i = kPKeySize; i < kNumColumns; i++) {
      std::string value(reinterpret_cast<const char*>(values[i]->data()), values[i]->type->size());
      ok &= storage_adapter_->Insert(storage_keys[i - kPKeySize], std::move(value));
    }
    return ok;
  }

  bool Delete(const std::vector<ScalarPtr>& pkey) {
    bool ok = true;
    auto storage_keys = MakeStorageKeys(pkey);
    for (auto& key : storage_keys) {
      ok &= storage_adapter_->Delete(std::move(key));
    }
    return ok;
  }

  /**
   * Let pkey be the columns making up the primary key.
   * A storage key is composed from pkey, table id, and a column: <pkey[0], table_id, pkey[1..], col>
   */
  inline static std::vector<std::string> MakeStorageKeys(const std::vector<ScalarPtr>& values,
                                                         const std::vector<Column>& columns = {}) {
    const std::vector<Column>* columns_ptr = columns.empty() ? &non_pkey_columns_ : &columns;
    CHECK_GE(values.size(), kPKeySize) << "Number of values needs to be equal or larger than primary key size";
    size_t storage_key_size = sizeof(TableId) + sizeof(Column);
    for (size_t i = 0; i < kPKeySize; i++) {
      ValidateType(values[i], static_cast<Column>(i));
      storage_key_size += values[i]->type->size();
    }

    std::string storage_key;
    storage_key.reserve(storage_key_size);
    // The first value is used for partitioning
    storage_key.append(reinterpret_cast<const char*>(values[0]->data()), values[0]->type->size());
    // Table id
    storage_key.append(reinterpret_cast<const char*>(&Schema::kId), sizeof(TableId));
    // The rest of pkey
    for (size_t i = 1; i < kPKeySize; i++) {
      storage_key.append(reinterpret_cast<const char*>(values[i]->data()), values[i]->type->size());
    }
    storage_key.resize(storage_key_size);

    std::vector<std::string> keys;
    size_t col_offset = storage_key.size() - sizeof(Column);
    for (auto col : *columns_ptr) {
      storage_key.replace(col_offset, sizeof(Column), reinterpret_cast<const char*>(&col), sizeof(Column));
      keys.push_back(storage_key);
    }

    return keys;
  }

  inline static void PrintRows(const std::vector<std::vector<ScalarPtr>>& rows, const std::vector<Column>& cols = {}) {
    if (rows.empty()) {
      return;
    }

    auto columns = cols;
    if (columns.empty()) {
      for (size_t i = 0; i < Schema::kNumColumns; i++) {
        columns.push_back(static_cast<Column>(i));
      }
    }

    for (const auto& row : rows) {
      CHECK_EQ(row.size(), columns.size()) << "Number of values does not match number of columns";
      bool first = true;
      for (size_t i = 0; i < columns.size(); i++) {
        ValidateType(row[i], columns[i]);
        if (!first) {
          std::cout << " | ";
        }
        std::cout << row[i]->to_string();
        first = false;
      }
      std::cout << std::endl;
    }
  }

 private:
  inline static void ValidateType(const ScalarPtr& val, Column col) {
    const auto& value_type = val->type;
    const auto& col_type = Schema::ColumnTypes[static_cast<size_t>(col)];
    CHECK(value_type->name() == col_type->name())
        << "Invalid column type. Value type: " << value_type->to_string() << ". Column type: " << col_type->to_string();
  }

  StorageAdapterPtr storage_adapter_;

  // Column offsets within a storage value
  inline static std::vector<Column> non_pkey_columns_;
  inline static std::array<size_t, kNumColumns> column_offsets_;
  inline static bool column_offsets_initialized_ = false;

  inline static void InitializeColumnOffsets() {
    if (column_offsets_initialized_) {
      return;
    }
    // First columns are primary keys so are not stored in the value portion
    for (size_t i = 0; i < kPKeySize; i++) {
      column_offsets_[i] = 0;
    }
    size_t offset = 0;
    for (size_t i = kPKeySize; i < kNumColumns; i++) {
      column_offsets_[i] = offset;
      offset += Schema::ColumnTypes[i]->size();
      non_pkey_columns_.push_back(Column(i));
    }
    column_offsets_initialized_ = true;
  }
};

#define ARRAY(...) __VA_ARGS__
#define SCHEMA(NAME, ID, NUM_COLUMNS, PKEY_SIZE, COLUMNS, COLUMN_TYPES)                                  \
  struct NAME {                                                                                          \
    static constexpr TableId kId = ID;                                                                   \
    static constexpr size_t kNumColumns = NUM_COLUMNS;                                                   \
    static constexpr size_t kPKeySize = PKEY_SIZE;                                                       \
    static constexpr size_t kNonPKeySize = kNumColumns - kPKeySize;                                      \
    enum struct Column : int8_t { COLUMNS };                                                             \
    inline static const std::array<std::shared_ptr<DataType>, kNumColumns> ColumnTypes = {COLUMN_TYPES}; \
  }

// clang-format off

SCHEMA(WarehouseSchema,
       TableId::WAREHOUSE,
       5, // NUM_COLUMNS
       1, // PKEY_SIZE
       ARRAY(ID,
             NAME,
             ADDRESS, // STREET_1, STREET_2, CITY, STATE, ZIP
             TAX,
             YTD),
       ARRAY(Int32Type::Get(), 
             FixedTextType<10>::Get(),
             FixedTextType<71>::Get(),
             Int32Type::Get(),
             Int64Type::Get()));

SCHEMA(DistrictSchema,
       TableId::DISTRICT,
       7, // NUM_COLUMNS
       2,  // PKEY_SIZE
       ARRAY(W_ID,
             ID,
             NAME,
             ADDRESS, // STREET_1, STREET_2, CITY, STATE, ZIP
             TAX,
             YTD,
             NEXT_O_ID), 
       ARRAY(Int32Type::Get(),
             Int8Type::Get(),
             FixedTextType<10>::Get(),
             FixedTextType<71>::Get(),
             Int32Type::Get(),
             Int64Type::Get(),
             Int32Type::Get()));

SCHEMA(CustomerSchema,
       TableId::CUSTOMER,
       17, // NUM_COLUMNS
       3,  // PKEY_SIZE
       ARRAY(W_ID,
             D_ID,
             ID,
             FIRST,
             MIDDLE,
             LAST,
             ADDRESS, // STREET_1, STREET_2, CITY, STATE, ZIP
             PHONE,
             SINCE,
             CREDIT,
             CREDIT_LIM,
             DISCOUNT,
             BALANCE,
             YTD_PAYMENT,
             PAYMENT_CNT,
             DELIVERY_CNT,
             DATA), 
       ARRAY(Int32Type::Get(),
             Int8Type::Get(),
             Int32Type::Get(),
             FixedTextType<16>::Get(),
             FixedTextType<2>::Get(),
             FixedTextType<16>::Get(),
             FixedTextType<71>::Get(),
             FixedTextType<16>::Get(),
             Int64Type::Get(),
             FixedTextType<2>::Get(),
             Int64Type::Get(),
             Int32Type::Get(),
             Int64Type::Get(),
             Int64Type::Get(),
             Int16Type::Get(),
             Int16Type::Get(),
             FixedTextType<250>::Get()));

SCHEMA(HistorySchema,
       TableId::HISTORY,
       9, // NUM_COLUMNS
       4, // PKEY_SIZE
       ARRAY(W_ID,
             D_ID,
             C_ID,
             ID,
             C_D_ID,
             C_W_ID,
             DATE,
             AMOUNT,
             DATA), 
       ARRAY(Int32Type::Get(),
             Int8Type::Get(),
             Int32Type::Get(),
             Int32Type::Get(),
             Int8Type::Get(),
             Int32Type::Get(),
             Int64Type::Get(),
             Int32Type::Get(),
             FixedTextType<24>::Get()));

SCHEMA(NewOrderSchema,
       TableId::NEW_ORDER,
       3, // NUM_COLUMNS
       3, // PKEY_SIZE
       ARRAY(W_ID,
             D_ID,
             O_ID), 
       ARRAY(Int32Type::Get(),
             Int8Type::Get(),
             Int32Type::Get()));

SCHEMA(OrderSchema,
       TableId::ORDER,
       8, // NUM_COLUMNS
       3, // PKEY_SIZE
       ARRAY(W_ID,
             D_ID,
             ID,
             C_ID,
             ENTRY_D,
             CARRIER_ID,
             OL_CNT,
             ALL_LOCAL), 
       ARRAY(Int32Type::Get(),
             Int8Type::Get(),
             Int32Type::Get(),
             Int32Type::Get(),
             Int64Type::Get(),
             Int8Type::Get(),
             Int8Type::Get(),
             Int8Type::Get()));

SCHEMA(OrderLineSchema,
       TableId::ORDER_LINE,
       10, // NUM_COLUMNS
       4,  // PKEY_SIZE
       ARRAY(W_ID,
             D_ID,
             O_ID,
             NUMBER,
             I_ID,
             SUPPLY_W_ID,
             DELIVERY_D,
             QUANTITY,
             AMOUNT,
             DIST_INFO), 
       ARRAY(Int32Type::Get(),
             Int8Type::Get(),
             Int32Type::Get(),
             Int8Type::Get(),
             Int32Type::Get(),
             Int32Type::Get(),
             Int64Type::Get(),
             Int8Type::Get(),
             Int32Type::Get(),
             FixedTextType<24>::Get()));

SCHEMA(ItemSchema,
       TableId::ITEM,
       6, // NUM_COLUMNS
       2, // PKEY_SIZE
       ARRAY(W_ID,
             ID,
             IM_ID,
             NAME,
             PRICE,
             DATA), 
       ARRAY(Int32Type::Get(),
             Int32Type::Get(),
             Int32Type::Get(),
             FixedTextType<24>::Get(),
             Int32Type::Get(),
             FixedTextType<50>::Get()));

SCHEMA(StockSchema,
       TableId::STOCK,
       17, // NUM_COLUMNS
       2,  // PKEY_SIZE
       ARRAY(W_ID,
             I_ID,
             QUANTITY,
             DIST_01,
             DIST_02,
             DIST_03,
             DIST_04,
             DIST_05,
             DIST_06,
             DIST_07,
             DIST_08,
             DIST_09,
             DIST_10,
             YTD,
             ORDER_CNT,
             REMOTE_CNT,
             DATA), 
       ARRAY(Int32Type::Get(),
             Int32Type::Get(),
             Int16Type::Get(),
             FixedTextType<24>::Get(),
             FixedTextType<24>::Get(),
             FixedTextType<24>::Get(),
             FixedTextType<24>::Get(),
             FixedTextType<24>::Get(),
             FixedTextType<24>::Get(),
             FixedTextType<24>::Get(),
             FixedTextType<24>::Get(),
             FixedTextType<24>::Get(),
             FixedTextType<24>::Get(),
             Int32Type::Get(),
             Int16Type::Get(),
             Int16Type::Get(),
             FixedTextType<50>::Get()));

// clang-format on

}  // namespace tpcc
}  // namespace slog