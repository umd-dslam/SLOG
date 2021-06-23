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

template <typename DType>
struct PrimitiveScalar : public Scalar {
  using CType = typename DType::CType;

  PrimitiveScalar(CType value) : Scalar(DType::Get()), value(value) {}

  const void* data() const override { return &value; }
  std::string to_string() const override { return std::to_string(value); }

  CType value{};
};

struct Int8Scalar : public PrimitiveScalar<Int8Type> {
  using PrimitiveScalar<Int8Type>::PrimitiveScalar;
};

struct Int16Scalar : public PrimitiveScalar<Int16Type> {
  using PrimitiveScalar<Int16Type>::PrimitiveScalar;
};

struct Int32Scalar : public PrimitiveScalar<Int32Type> {
  using PrimitiveScalar<Int32Type>::PrimitiveScalar;
};

struct Int64Scalar : public PrimitiveScalar<Int64Type> {
  using PrimitiveScalar<Int64Type>::PrimitiveScalar;
};

struct FixedTextScalar : public Scalar {
  FixedTextScalar(const std::shared_ptr<DataType>& type, const std::string& buffer) : Scalar(type), buffer(buffer) {}
  FixedTextScalar(const std::shared_ptr<DataType>& type, std::string&& buffer)
      : Scalar(type), buffer(std::move(buffer)) {}

  const void* data() const override { return buffer.data(); }

  std::string to_string() const override { return buffer; }

  std::string buffer;
};

inline std::shared_ptr<Scalar> MakeInt8Scalar(int8_t data) { return std::make_shared<Int8Scalar>(data); }

inline std::shared_ptr<Scalar> MakeInt16Scalar(int16_t data) { return std::make_shared<Int16Scalar>(data); }

inline std::shared_ptr<Scalar> MakeInt32Scalar(int32_t data) { return std::make_shared<Int32Scalar>(data); }

inline std::shared_ptr<Scalar> MakeInt64Scalar(int64_t data) { return std::make_shared<Int64Scalar>(data); }

inline std::shared_ptr<Scalar> MakeFixedTextScalar(const std::shared_ptr<DataType>& type, const std::string& data) {
  CHECK(type->name() == DataTypeName::FIXED_TEXT);
  CHECK_EQ(data.size(), type->size()) << "Size does not match: \"" << data << "\". Need size = " << type->size();
  return std::make_shared<FixedTextScalar>(type, data);
}

template <size_t Width>
inline std::shared_ptr<Scalar> MakeFixedTextScalar(const std::string& data) {
  return MakeFixedTextScalar(FixedTextType<Width>::Get(), data);
}

inline std::shared_ptr<Scalar> MakeScalar(const std::shared_ptr<DataType>& type, const void* data) {
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
std::shared_ptr<Scalar> MakeScalar(const std::shared_ptr<DataType>& type, T data) {
  return MakeScalar(type, reinterpret_cast<const void*>(&data));
}

std::shared_ptr<Scalar> MakeScalar(const std::shared_ptr<DataType>& type, const char* data) {
  return MakeFixedTextScalar(type, std::string{data});
}

template <typename DType>
bool operator==(const PrimitiveScalar<DType>& s1, const PrimitiveScalar<DType>& s2) {
  return s1.value == s2.value;
}

bool operator==(const FixedTextScalar& s1, const FixedTextScalar& s2) { return s1.buffer == s2.buffer; }

bool operator==(const Scalar& s1, const Scalar& s2) {
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