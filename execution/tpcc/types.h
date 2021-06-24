#pragma once

#include <memory>

namespace slog {
namespace tpcc {

enum class DataTypeName { INT8, INT16, INT32, INT64, FIXED_TEXT };

class DataType {
 public:
  DataType() = default;
  virtual ~DataType() = default;
  virtual DataTypeName name() const = 0;
  virtual std::size_t size() const = 0;
  virtual std::string to_string() const = 0;
};

template <typename BaseType>
class NumericDataType : public DataType {
 public:
  using CType = BaseType;
  std::size_t size() const override { return sizeof(BaseType); }
};

#define STRINGIFY(S) #S
#define NUMERIC_TYPE(KLASS, NAME, CTYPE)                               \
  class KLASS : public NumericDataType<CTYPE> {                        \
   public:                                                             \
    DataTypeName name() const override { return DataTypeName::NAME; }  \
    std::string to_string() const override { return STRINGIFY(NAME); } \
    inline static std::shared_ptr<DataType> Get() {                    \
      static auto result = std::make_shared<KLASS>();                  \
      return result;                                                   \
    }                                                                  \
  }

NUMERIC_TYPE(Int8Type, INT8, int8_t);
NUMERIC_TYPE(Int16Type, INT16, int16_t);
NUMERIC_TYPE(Int32Type, INT32, int32_t);
NUMERIC_TYPE(Int64Type, INT64, int64_t);

template <size_t Width>
class FixedTextType : public DataType {
 public:
  DataTypeName name() const override { return DataTypeName::FIXED_TEXT; }
  std::string to_string() const override { return "FIXED_TEXT<" + std::to_string(Width) + ">"; }
  size_t size() const override { return Width; }

  inline static std::shared_ptr<DataType> Get() {
    static auto result = std::make_shared<FixedTextType<Width>>();
    return result;
  }
};

inline bool operator==(const DataType& dt1, const DataType& dt2) {
  if (dt1.name() != dt2.name()) {
    return false;
  }
  if (dt1.name() == DataTypeName::FIXED_TEXT && dt1.size() != dt2.size()) {
    return false;
  }
  return true;
}

}  // namespace tpcc
}  // namespace slog