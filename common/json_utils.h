#pragma once

#include <functional>

#include "rapidjson/allocators.h"
#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace slog {

template <typename Container, typename ValueFn>
rapidjson::Value ToJsonArrayOfKeyValue(const Container& container, ValueFn value_fn,
                                       rapidjson::Document::AllocatorType& alloc) {
  rapidjson::Value json_array(rapidjson::kArrayType);
  for (const auto& [key, value] : container) {
    rapidjson::Value entry(rapidjson::kArrayType);
    entry.PushBack(key, alloc).PushBack(rapidjson::Value(value_fn(value)), alloc);
    json_array.PushBack(std::move(entry), alloc);
  }
  return json_array;
}

template <typename Container>
rapidjson::Value ToJsonArrayOfKeyValue(const Container& container, rapidjson::Document::AllocatorType& alloc) {
  return ToJsonArrayOfKeyValue(
      container, [](const auto& value) { return value; }, alloc);
}

template <typename Container, typename Function>
rapidjson::Value ToJsonArray(const Container& container, Function fn, rapidjson::Document::AllocatorType& alloc) {
  rapidjson::Value json_array(rapidjson::kArrayType);
  for (const auto& v : container) {
    json_array.PushBack(rapidjson::Value(fn(v)), alloc);
  }
  return json_array;
}

template <typename Container>
rapidjson::Value ToJsonArray(const Container& container, rapidjson::Document::AllocatorType& alloc) {
  return ToJsonArray(
      container, [](const auto& value) { return value; }, alloc);
}

const std::array<int, 11> kPctlLevels = {0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100};

template <typename Container>
rapidjson::Value Percentiles(Container& container, rapidjson::Document::AllocatorType& alloc) {
  rapidjson::Value pctls(rapidjson::kArrayType);
  if (container.empty()) {
    return pctls;
  }
  std::sort(container.begin(), container.end());
  for (auto p : kPctlLevels) {
    size_t i = p * (container.size() - 1) / 100;
    pctls.PushBack(container[i], alloc);
  }
  return pctls;
}

}  // namespace slog