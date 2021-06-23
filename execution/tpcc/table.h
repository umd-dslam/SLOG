#pragma once

#include <array>
#include <exception>
#include <unordered_map>
#include <vector>

#include "execution/tpcc/scalar.h"

namespace slog {
namespace tpcc {

using KeyValueTable = std::unordered_map<std::string, std::string>;
using KeyValueTablePtr = std::shared_ptr<KeyValueTable>;
using KeyList = std::vector<std::string>;
using KeyListPtr = std::shared_ptr<std::vector<std::string>>;

template <typename Schema>
class Table {
 public:
  using Column = typename Schema::Column;
  static constexpr size_t NumColumns = Schema::NumColumns;
  static constexpr size_t PKeySize = Schema::PKeySize;

  Table(KeyValueTablePtr key_value_table, KeyListPtr deleted_keys)
      : key_value_table_(key_value_table), deleted_keys_(deleted_keys) {
    InitializeColumnOffsets();
  }

  std::vector<ScalarPtr> Select(const std::vector<ScalarPtr>& pkey, const std::vector<Column>& columns = {}) {
    auto storage_key = MakeStorageKey(pkey);
    auto it = key_value_table_->find(storage_key);
    if (it == key_value_table_->end()) {
      return {};
    }

    auto storage_value = it->second.data();
    std::vector<ScalarPtr> result;
    result.reserve(columns.size());

    // If no column is provided, select ALL columns
    if (columns.empty()) {
      result.insert(result.end(), pkey.begin(), pkey.end());
      for (size_t i = PKeySize; i < NumColumns; i++) {
        auto value = reinterpret_cast<void*>(storage_value + column_offsets_[i]);
        result.push_back(MakeScalar(Schema::ColumnTypes[i], value));
      }
    } else {
      for (auto c : columns) {
        auto i = static_cast<size_t>(c);
        if (i < PKeySize) {
          result.push_back(pkey[i]);
        } else {
          auto value = reinterpret_cast<void*>(storage_value + column_offsets_[i]);
          result.push_back(MakeScalar(Schema::ColumnTypes[i], value));
        }
      }
    }

    return result;
  }

  bool Update(const std::vector<ScalarPtr>& pkey, const std::vector<Column>& columns,
              const std::vector<ScalarPtr>& values) {
    if (columns.size() != values.size()) {
      throw std::runtime_error("Number of values does not match number of columns");
    }

    auto storage_key = MakeStorageKey(pkey);
    auto it = key_value_table_->find(storage_key);
    if (it == key_value_table_->end()) {
      return false;
    }

    auto& storage_value = it->second;
    for (size_t i = 0; i < values.size(); i++) {
      auto c = columns[i];
      const auto& v = values[i];
      auto offset = column_offsets_[static_cast<size_t>(c)];
      storage_value.replace(offset, v->type->size(), reinterpret_cast<const char*>(v->data()));
    }

    return true;
  }

  bool Insert(const std::vector<ScalarPtr>& values) {
    if (values.size() != NumColumns) {
      throw std::runtime_error("Number of values does not match number of columns");
    }

    size_t storage_value_size = 0;
    for (size_t i = PKeySize; i < NumColumns; i++) {
      ValidateType(values[i], static_cast<Column>(i));
      storage_value_size += values[i]->type->size();
    }

    std::string storage_value;
    storage_value.reserve(storage_value_size);
    for (size_t i = PKeySize; i < NumColumns; i++) {
      storage_value.append(reinterpret_cast<const char*>(values[i]->data()), values[i]->type->size());
    }

    auto res = key_value_table_->emplace(MakeStorageKey(values), std::move(storage_value));
    return res.second;
  }

  void Delete(const std::vector<ScalarPtr>& pkey) {
    auto storage_key = MakeStorageKey(pkey);
    key_value_table_->erase(storage_key);
    deleted_keys_->push_back(storage_key);
  }

  /**
   * Creates storage key from the first PKeySize values
   */
  inline static std::string MakeStorageKey(const std::vector<ScalarPtr>& values) {
    if (values.size() < PKeySize) {
      throw std::runtime_error("Number of values needs to be equal or larger than primary key size");
    }
    size_t storage_key_size = 0;
    for (size_t i = 0; i < PKeySize; i++) {
      ValidateType(values[i], static_cast<Column>(i));
      storage_key_size += values[i]->type->size();
    }

    std::string storage_key;
    storage_key.reserve(storage_key_size);
    for (size_t i = 0; i < PKeySize; i++) {
      storage_key.append(reinterpret_cast<const char*>(values[i]->data()), values[i]->type->size());
    }

    return storage_key;
  }

  inline static void ValidateType(const ScalarPtr& val, Column col) {
    if (val->type->name() != Schema::ColumnTypes[static_cast<size_t>(col)]->name()) {
      throw std::runtime_error("Invalid column type");
    }
  }

 private:
  KeyValueTablePtr key_value_table_;
  KeyListPtr deleted_keys_;

  // Column offsets within a storage value
  inline static std::array<size_t, NumColumns> column_offsets_;
  inline static bool column_offsets_initialized_ = false;

  inline static void InitializeColumnOffsets() {
    if (column_offsets_initialized_) {
      return;
    }
    // First columns are primary keys so are not stored in the value portion
    for (size_t i = 0; i < PKeySize; i++) {
      column_offsets_[i] = 0;
    }
    size_t offset = 0;
    for (size_t i = PKeySize; i < NumColumns; i++) {
      column_offsets_[i] = offset;
      offset += Schema::ColumnTypes[i]->size();
    }
  }
};

#define ARRAY(...) __VA_ARGS__
#define SCHEMA(NAME, NUM_COLUMNS, PKEY_SIZE, COLUMNS, COLUMN_TYPES)                                     \
  struct NAME {                                                                                         \
    static constexpr size_t NumColumns = NUM_COLUMNS;                                                   \
    static constexpr size_t PKeySize = PKEY_SIZE;                                                       \
    enum class Column { COLUMNS };                                                                      \
    inline static const std::array<std::shared_ptr<DataType>, NumColumns> ColumnTypes = {COLUMN_TYPES}; \
  }

// clang-format off

SCHEMA(WarehouseSchema,
       9, // NUM_COLUMNS
       1, // PKEY_SIZE
       ARRAY(ID,
             NAME,
             STREET_1,
             STREET_2,
             CITY,
             STATE,
             ZIP,
             TAX,
             YTD),
       ARRAY(Int32Type::Get(), 
             FixedTextType<10>::Get(),
             FixedTextType<20>::Get(),
             FixedTextType<20>::Get(),
             FixedTextType<20>::Get(),
             FixedTextType<2>::Get(),
             FixedTextType<9>::Get(),
             Int32Type::Get(),
             Int64Type::Get()));

SCHEMA(DistrictSchema,
       11, // NUM_COLUMNS
       2,  // PKEY_SIZE
       ARRAY(W_ID,
             ID,
             NAME,
             STREET_1,
             STREET_2,
             CITY,
             STATE,
             ZIP,
             TAX,
             YTD,
             NEXT_O_ID), 
       ARRAY(Int32Type::Get(),
             Int8Type::Get(),
            //  FixedTextType<10>::Get(),
             FixedTextType<20>::Get(),
             FixedTextType<20>::Get(),
             FixedTextType<20>::Get(),
             FixedTextType<2>::Get(),
             FixedTextType<9>::Get(),
             Int32Type::Get(),
             Int64Type::Get(),
             Int32Type::Get()));

SCHEMA(CustomerSchema,
       21, // NUM_COLUMNS
       3,  // PKEY_SIZE
       ARRAY(W_ID,
             D_ID,
             ID,
             FIRST,
             MIDDLE,
             LAST,
             STREET_1,
             STREET_2,
             CITY,
             STATE,
             ZIP,
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
             FixedTextType<20>::Get(),
             FixedTextType<20>::Get(),
             FixedTextType<20>::Get(),
             FixedTextType<2>::Get(),
             FixedTextType<9>::Get(),
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
       3, // NUM_COLUMNS
       3, // PKEY_SIZE
       ARRAY(W_ID,
             D_ID,
             O_ID), 
       ARRAY(Int32Type::Get(),
             Int8Type::Get(),
             Int32Type::Get()));

SCHEMA(OrderSchema,
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