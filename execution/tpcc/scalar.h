#pragma once

#include <glog/logging.h>

#include "execution/tpcc/types.h"

namespace slog {
namespace tpcc {

struct Scalar {
  std::shared_ptr<DataType> type;

  virtual ~Scalar() = default;
  virtual const void* data() const = 0;
  virtual std::string to_string() const = 0;

 protected:
  Scalar(const std::shared_ptr<DataType>& type) : type(type) {}
};

using ScalarPtr = std::shared_ptr<Scalar>;

template <typename DType_>
struct PrimitiveScalar : public Scalar {
  using DType = DType_;
  using CType = typename DType::CType;

  PrimitiveScalar(CType value) : Scalar(DType::Get()), value(value) {}

  const void* data() const override { return &value; }
  std::string to_string() const override { return std::to_string(value); }

  CType value{};
};

#define NEW_PRIMITIVE_SCALAR(NAME)                                                                            \
  struct NAME##Scalar : public PrimitiveScalar<NAME##Type> {                                                  \
    using PrimitiveScalar<NAME##Type>::PrimitiveScalar;                                                       \
  };                                                                                                          \
  using NAME##ScalarPtr = std::shared_ptr<NAME##Scalar>;                                                      \
  inline std::shared_ptr<NAME##Scalar> Make##NAME##Scalar(NAME##Scalar::CType data = NAME##Scalar::CType()) { \
    return std::make_shared<NAME##Scalar>(data);                                                              \
  }

NEW_PRIMITIVE_SCALAR(Int8);
NEW_PRIMITIVE_SCALAR(Int16);
NEW_PRIMITIVE_SCALAR(Int32);
NEW_PRIMITIVE_SCALAR(Int64);

struct FixedTextScalar : public Scalar {
  FixedTextScalar(const std::shared_ptr<DataType>& type, const std::string& buffer) : Scalar(type), buffer(buffer) {}
  FixedTextScalar(const std::shared_ptr<DataType>& type, std::string&& buffer)
      : Scalar(type), buffer(std::move(buffer)) {}

  const void* data() const override { return buffer.data(); }

  std::string to_string() const override { return buffer; }

  std::string buffer;
};
using FixedTextScalarPtr = std::shared_ptr<FixedTextScalar>;

inline std::shared_ptr<FixedTextScalar> MakeFixedTextScalar(const std::shared_ptr<DataType>& type,
                                                            const std::string& data) {
  CHECK(type->name() == DataTypeName::FIXED_TEXT);
  CHECK_EQ(data.size(), type->size()) << "Size does not match: \"" << data << "\". Need size = " << type->size();
  return std::make_shared<FixedTextScalar>(type, data);
}

template <size_t Width>
inline std::shared_ptr<FixedTextScalar> MakeFixedTextScalar(const std::string& data) {
  return MakeFixedTextScalar(FixedTextType<Width>::Get(), data);
}

inline std::shared_ptr<FixedTextScalar> MakeFixedTextScalar() {
  return MakeFixedTextScalar(FixedTextType<0>::Get(), "");
}

inline ScalarPtr MakeScalar(const std::shared_ptr<DataType>& type, const void* data) {
  switch (type->name()) {
    case DataTypeName::INT8:
      return MakeInt8Scalar(*reinterpret_cast<const Int8Type::CType*>(data));
    case DataTypeName::INT16:
      return MakeInt16Scalar(*reinterpret_cast<const Int16Type::CType*>(data));
    case DataTypeName::INT32:
      return MakeInt32Scalar(*reinterpret_cast<const Int32Type::CType*>(data));
    case DataTypeName::INT64:
      return MakeInt64Scalar(*reinterpret_cast<const Int64Type::CType*>(data));
    case DataTypeName::FIXED_TEXT:
      return MakeFixedTextScalar(type, {static_cast<const char*>(data), type->size()});
  }
  return nullptr;
}

template <typename T, typename = std::enable_if_t<std::is_arithmetic<T>::value>>
ScalarPtr MakeScalar(const std::shared_ptr<DataType>& type, T data) {
  return MakeScalar(type, reinterpret_cast<const void*>(&data));
}

inline ScalarPtr MakeScalar(const std::shared_ptr<DataType>& type, const char* data) {
  return MakeFixedTextScalar(type, std::string{data});
}

template <typename T>
inline std::shared_ptr<T> UncheckedCast(const ScalarPtr& from) {
  return std::static_pointer_cast<T>(from);
}

template <typename DType>
bool operator==(const PrimitiveScalar<DType>& s1, const PrimitiveScalar<DType>& s2) {
  return s1.value == s2.value;
}

inline bool operator==(const FixedTextScalar& s1, const FixedTextScalar& s2) { return s1.buffer == s2.buffer; }

inline bool operator==(const Scalar& s1, const Scalar& s2) {
  if (s1.type != s2.type) {
    return false;
  }
  switch (s1.type->name()) {
    case DataTypeName::INT8:
      return static_cast<const Int8Scalar&>(s1) == static_cast<const Int8Scalar&>(s2);
    case DataTypeName::INT16:
      return static_cast<const Int16Scalar&>(s1) == static_cast<const Int16Scalar&>(s2);
    case DataTypeName::INT32:
      return static_cast<const Int32Scalar&>(s1) == static_cast<const Int32Scalar&>(s2);
    case DataTypeName::INT64:
      return static_cast<const Int64Scalar&>(s1) == static_cast<const Int64Scalar&>(s2);
    case DataTypeName::FIXED_TEXT:
      return static_cast<const FixedTextScalar&>(s1) == static_cast<const FixedTextScalar&>(s2);
  }
  return false;
}

}  // namespace tpcc
}  // namespace slog