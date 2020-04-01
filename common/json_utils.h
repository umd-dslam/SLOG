#pragma once

#include <functional>
#include <unordered_map>
#include <unordered_set>

#include "third_party/rapidjson/allocators.h"
#include "third_party/rapidjson/document.h"
#include "third_party/rapidjson/rapidjson.h"
#include "third_party/rapidjson/stringbuffer.h"
#include "third_party/rapidjson/writer.h"

namespace slog {

template<
    typename Container,
    typename BaseAllocator,
    typename ValueFn>
rapidjson::Value ToJsonArrayOfKeyValue(
    const Container& container,
    ValueFn value_fn,
    rapidjson::MemoryPoolAllocator<BaseAllocator>& alloc) {
  rapidjson::Value json_array(rapidjson::kArrayType);
  for (const auto& pair : container) {
    rapidjson::Value entry(rapidjson::kArrayType);
    entry
        .PushBack(pair.first, alloc)
        .PushBack(rapidjson::Value(value_fn(pair.second)), alloc);
    json_array.PushBack(std::move(entry), alloc);
  }
  return json_array;
}

template<typename Container, typename BaseAllocator>
rapidjson::Value ToJsonArrayOfKeyValue(
    const Container& container,
    rapidjson::MemoryPoolAllocator<BaseAllocator>& alloc) {
  return ToJsonArrayOfKeyValue(
      container,
      [](const auto& value) {
        return value;
      },
      alloc);
}

template<
    typename Container,
    typename BaseAllocator,
    typename Function>
rapidjson::Value ToJsonArray(
    const Container& container,
    Function fn,
    rapidjson::MemoryPoolAllocator<BaseAllocator>& alloc) {
  rapidjson::Value json_array(rapidjson::kArrayType);
  for (const auto& v : container) {
    json_array.PushBack(rapidjson::Value(fn(v)), alloc);
  }
  return json_array;
}

template<
    typename Container,
    typename BaseAllocator>
rapidjson::Value ToJsonArray(
    const Container& container,
    rapidjson::MemoryPoolAllocator<BaseAllocator>& alloc) {
  return ToJsonArray(
      container,
      [](const auto& value) {
        return value;
      },
      alloc);
}

} // namespace slog