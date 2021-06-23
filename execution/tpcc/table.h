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
  static constexpr size_t NumPKeys = Schema::NumPKeys;

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

    if (columns.empty()) {
      result.insert(result.end(), pkey.begin(), pkey.end());
      for (size_t i = NumPKeys; i < NumColumns; i++) {
        auto value = reinterpret_cast<void*>(storage_value + column_offsets_[i]);
        result.push_back(MakeScalar(Schema::ColumnTypes[i], value));
      }
    } else {
      for (auto c : columns) {
        auto i = static_cast<size_t>(c);
        if (i < NumPKeys) {
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
    for (size_t i = NumPKeys; i < NumColumns; i++) {
      ValidateType(values[i], static_cast<Column>(i));
      storage_value_size += values[i]->type->size();
    }

    std::string storage_value;
    storage_value.reserve(storage_value_size);
    for (size_t i = NumPKeys; i < NumColumns; i++) {
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
   * Creates storage key from the first NumPKeys values
   */
  inline static std::string MakeStorageKey(const std::vector<ScalarPtr>& values) {
    if (values.size() < NumPKeys) {
      throw std::runtime_error("Number of values need to be equal or larger than primary key size");
    }
    size_t storage_key_size = 0;
    for (size_t i = 0; i < NumPKeys; i++) {
      ValidateType(values[i], static_cast<Column>(i));
      storage_key_size += values[i]->type->size();
    }

    std::string storage_key;
    storage_key.reserve(storage_key_size);
    for (size_t i = 0; i < NumPKeys; i++) {
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
    for (size_t i = 0; i < NumPKeys; i++) {
      column_offsets_[i] = 0;
    }
    size_t offset = 0;
    for (size_t i = NumPKeys; i < NumColumns; i++) {
      column_offsets_[i] = offset;
      offset += Schema::ColumnTypes[i]->size();
    }
  }
};

struct WarehouseSchema {
  enum class Column { ID, NAME, STREET_1, STREET_2, CITY, STATE, ZIP, TAX, YTD };
  static constexpr size_t NumColumns = 9;
  static constexpr size_t NumPKeys = 1;
  inline static const std::array<std::shared_ptr<DataType>, NumColumns> ColumnTypes = {
      Int32Type::Get(),         FixedTextType<10>::Get(), FixedTextType<20>::Get(),
      FixedTextType<20>::Get(), FixedTextType<20>::Get(), FixedTextType<2>::Get(),
      FixedTextType<9>::Get(),  Int32Type::Get(),         Int32Type::Get(),
  };
};

}  // namespace tpcc
}  // namespace slog