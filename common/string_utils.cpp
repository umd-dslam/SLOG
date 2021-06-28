#include "common/string_utils.h"

#include <algorithm>

namespace slog {

using std::string;
using std::vector;

size_t NextToken(string& token, const string& str, const string& delims, size_t pos) {
  auto start = str.find_first_not_of(delims, pos);
  if (start == string::npos) {
    token = "";
    return string::npos;
  }
  auto end = str.find_first_of(delims, start);
  if (end == string::npos) {
    end = str.length();
  }
  token = str.substr(start, end - start);
  return start + token.length();
}

size_t NextNTokens(vector<string>& tokens, const string& str, const string& delims, size_t n, size_t pos) {
  tokens.clear();
  for (size_t i = 0; i < n; i++) {
    string token;
    pos = NextToken(token, str, delims, pos);
    if (pos == string::npos) {
      tokens.clear();
      return string::npos;
    }
    tokens.push_back(std::move(token));
  }
  return pos;
}

string Trim(string str) {
  // trim left
  auto it = str.begin();
  while (it != str.end() && std::isspace(*it)) {
    it++;
  }
  str.erase(str.begin(), it);

  // trim right
  auto rit = str.rbegin();
  while (rit != str.rend() && std::isspace(*rit)) {
    rit++;
  }
  str.erase(rit.base(), str.end());

  return str;
}

vector<string> Split(const std::string& str, const std::string& delims) {
  vector<string> res;
  string token;
  size_t pos = NextToken(token, str, delims, 0);
  while (pos != std::string::npos) {
    res.push_back(token);
    pos = NextToken(token, str, delims, pos);
  }
  return res;
}

const std::string RandomStringGenerator::kCharacters("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz ");

RandomStringGenerator::RandomStringGenerator(int seed, size_t pool_size) : rg_(seed), rnd_str_pool_offset_(0) {
  rnd_str_pool_.reserve(pool_size);
  std::uniform_int_distribution<uint32_t> char_rnd(0, kCharacters.size() - 1);
  for (size_t i = 0; i < pool_size; i++) {
    rnd_str_pool_.push_back(kCharacters[char_rnd(rg_)]);
  }
}

std::string RandomStringGenerator::operator()(size_t len) {
  if (rnd_str_pool_offset_ + len >= rnd_str_pool_.size()) {
    rnd_str_pool_offset_ = 0;
  }
  auto res = rnd_str_pool_.substr(rnd_str_pool_offset_, len);
  rnd_str_pool_offset_ += len;
  return res;
}

}  // namespace slog