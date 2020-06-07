#pragma once

#include <map>
#include <sstream>
#include <random>
#include <unordered_map>

#include "common/types.h"
#include "common/string_utils.h"
#include "proto/transaction.pb.h"

namespace slog {

using RawParamMap = std::unordered_map<std::string, std::string>; 

class WorkloadParams {
public:
  WorkloadParams(const RawParamMap& default_params) {
    raw_params_ = default_params;
  }

  void Update(const std::string& params_str) {
    auto new_params_ = Parse(params_str);
    for (const auto& pair : new_params_) {
      if (raw_params_.count(pair.first) == 0) {
        std::ostringstream ss;
        ss << "Invalid param for current workload: " << pair.first
           << "\nDefault params: " << ToString();
        throw std::runtime_error(ss.str());
      }
      raw_params_[pair.first] = pair.second;
    }
  }

  double GetDouble(const std::string& key) const {
    CheckKeyExists(key);
    return std::stod(raw_params_.at(key));
  }

  int GetInt(const std::string& key) const {
    CheckKeyExists(key);
    return std::stoi(raw_params_.at(key));
  }

  uint32_t GetUInt32(const std::string& key) const {
    CheckKeyExists(key);
    return std::stoul(raw_params_.at(key));
  }

  uint64_t GetUInt64(const std::string& key) const {
    CheckKeyExists(key);
    return std::stoull(raw_params_.at(key));
  }

  std::string GetString(const std::string& key) const {
    CheckKeyExists(key);
    return raw_params_.at(key);
  }

  std::string ToString() const {
    std::ostringstream ss;
    for (const auto& param : raw_params_) {
      ss << param.first << " = " << param.second << "; ";
    }
    return ss.str();
  }

private:

  RawParamMap Parse(const std::string& params_str) {
    RawParamMap map;
    std::string token;
    size_t pos = 0;
    while ((pos = NextToken(token, params_str, ";,", pos)) != std::string::npos) {
      auto eq_pos = token.find_first_of("=");
      if (eq_pos == std::string::npos) {
        throw std::runtime_error("Invalid param entry: " + token);
      }
      auto key = Trim(token.substr(0, eq_pos));
      map[key] = Trim(token.substr(eq_pos + 1));
    }
    return map;
  }

  void CheckKeyExists(const std::string& key) const {
    if (raw_params_.count(key) == 0) {
      throw std::runtime_error("Key does not exist");
    }
  }

  RawParamMap raw_params_;
};

struct TransactionProfile {
  TxnId client_txn_id;
  bool is_multi_home;
  bool is_multi_partition;

  std::map<Key, uint32_t> key_to_partition;
  std::map<Key, uint32_t> key_to_home;
  std::map<Key, bool> is_hot_record;
  std::map<Key, bool> is_write_record;
};

/**
 * Base class for a workload generator
 */
class WorkloadGenerator {
public:
  WorkloadGenerator(
      const RawParamMap& default_params,
      const std::string& params_str) : params_(default_params) {
    params_.Update(params_str);
  }

  /**
   * Gets the next transaction in the workload
   */
  virtual std::pair<Transaction*, TransactionProfile> NextTransaction() = 0;

  std::string GetParamsStr() {
    return params_.ToString();
  }

  static const RawParamMap MergeParams(const RawParamMap& p1, const RawParamMap& p2) {
    RawParamMap params;
    params.insert(p1.begin(), p1.end());
    params.insert(p2.begin(), p2.end());
    return params;
  }

protected:
  WorkloadParams params_;
};

/**
 * Chooses without replacement k elements from [0, n)
 */
inline std::vector<uint32_t> Choose(uint32_t n, uint32_t k, std::mt19937& re) {
  if (n == 0) {
    return {};
  }
  if (k == 1) {
    // For k = 1, it is faster to pick a random key than shuffling
    // the whole vector and pick the first key.
    std::uniform_int_distribution<uint32_t> dis(0, n - 1);
    return {dis(re)};
  }
  std::vector<uint32_t> a(n);
  std::iota(a.begin(), a.end(), 0);
  shuffle(a.begin(), a.end(), re);
  return {a.begin(), a.begin() + std::min(n, k)};
}

/**
 * Randomly picks an element from a vector uniformly
 */
template<typename T>
T PickOne(const std::vector<T>& v, std::mt19937& re) {
  if (v.empty()) {
    throw std::runtime_error("Cannot pick from an empty container");
  }
  auto i = Choose(v.size(), 1, re)[0];
  return v[i];
}

const std::string CHARACTERS("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz");

/**
 * Generates a random string of length n
 */
inline std::string RandomString(size_t n, std::mt19937& re) {
  std::string s;
  for (size_t i = 0; i < n; i++) {
    auto k = Choose(CHARACTERS.size(), 1, re)[0];
    s += CHARACTERS[k];
  }
  return s;
}

class KeyList {
public:
  KeyList(size_t num_hot_keys = 0) : num_hot_keys_(num_hot_keys){}

  void AddKey(Key key) {
    if (hot_keys_.size() < num_hot_keys_) {
      hot_keys_.push_back(key);
      return;
    }
    cold_keys_.push_back(key);
  }
  Key GetRandomHotKey() {
    if (hot_keys_.empty()) {
      throw std::runtime_error("There is no hot key to pick from. Please check your params");
    }
    return PickOne(hot_keys_, re_);
  }
  Key GetRandomColdKey() {
    if (cold_keys_.empty()) {
      throw std::runtime_error("There is no cold key to pick from. Please check your params");
    }
    return PickOne(cold_keys_, re_);
  }

private:
  size_t num_hot_keys_;
  std::vector<Key> cold_keys_;
  std::vector<Key> hot_keys_;

  std::mt19937 re_;
};

} // namespace slog