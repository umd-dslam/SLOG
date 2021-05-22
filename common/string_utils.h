#pragma once

#include <sstream>
#include <string>
#include <vector>

namespace slog {

size_t NextToken(std::string& token, const std::string& str, const std::string& delims, size_t pos = 0);

size_t NextNTokens(std::vector<std::string>& tokens, const std::string& str, const std::string& delims, size_t n = 1,
                   size_t pos = 0);

std::string Trim(std::string str);

std::vector<std::string> Split(const std::string& str, const std::string& delims);

template <template <typename, typename...> class Container, typename T, typename R>
std::string Join(const Container<std::pair<T, R>>& parts, char delim = ';') {
  std::ostringstream ss;
  bool first = true;
  for (const auto& p : parts) {
    if (!first) {
      ss << delim;
    } else {
      first = false;
    }
    ss << p.first << ":" << p.second;
  }
  return ss.str();
}

template <typename Container>
std::string Join(const Container& parts, char delim = ';') {
  std::ostringstream ss;
  bool first = true;
  for (const auto& p : parts) {
    if (!first) {
      ss << delim;
    } else {
      first = false;
    }
    ss << p;
  }
  return ss.str();
}

}  // namespace slog