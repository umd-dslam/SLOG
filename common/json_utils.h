#pragma once

#include <functional>

#include "rapidjson/allocators.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace slog {

template <typename Container, typename BaseAllocator, typename ValueFn>
rapidjson::Value ToJsonArrayOfKeyValue(const Container& container, ValueFn value_fn,
                                       rapidjson::MemoryPoolAllocator<BaseAllocator>& alloc) {
  rapidjson::Value json_array(rapidjson::kArrayType);
  for (const auto& [key, value] : container) {
    rapidjson::Value entry(rapidjson::kArrayType);
    entry.PushBack(key, alloc).PushBack(rapidjson::Value(value_fn(value)), alloc);
    json_array.PushBack(std::move(entry), alloc);
  }
  return json_array;
}

template <typename Container, typename BaseAllocator>
rapidjson::Value ToJsonArrayOfKeyValue(const Container& container,
                                       rapidjson::MemoryPoolAllocator<BaseAllocator>& alloc) {
  return ToJsonArrayOfKeyValue(
      container, [](const auto& value) { return value; }, alloc);
}

template <typename Container, typename BaseAllocator, typename Function>
rapidjson::Value ToJsonArray(const Container& container, Function fn,
                             rapidjson::MemoryPoolAllocator<BaseAllocator>& alloc) {
  rapidjson::Value json_array(rapidjson::kArrayType);
  for (const auto& v : container) {
    json_array.PushBack(rapidjson::Value(fn(v)), alloc);
  }
  return json_array;
}

template <typename Container, typename BaseAllocator>
rapidjson::Value ToJsonArray(const Container& container, rapidjson::MemoryPoolAllocator<BaseAllocator>& alloc) {
  return ToJsonArray(
      container, [](const auto& value) { return value; }, alloc);
}

}  // namespace slog