#pragma once

#include <string>
#include <vector>

namespace slog {

size_t NextToken(std::string& token, const std::string& str, const std::string& delims, size_t pos = 0);

size_t NextNTokens(std::vector<std::string>& tokens, const std::string& str, const std::string& delims, size_t n = 1,
                   size_t pos = 0);

std::string Trim(std::string str);

std::vector<std::string> Split(const std::string& str, const std::string& delims);

}  // namespace slog