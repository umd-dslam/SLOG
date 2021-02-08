#pragma once

#include <algorithm>
#include <map>
#include <random>
#include <sstream>
#include <unordered_map>

#include "common/configuration.h"
#include "common/string_utils.h"
#include "common/types.h"
#include "proto/transaction.pb.h"

namespace slog {

using RawParamMap = std::unordered_map<std::string, std::string>;

class WorkloadParams {
 public:
  WorkloadParams(const RawParamMap& default_params) { raw_params_ = default_params; }

  void Update(const std::string& params_str) {
    auto new_params_ = Parse(params_str);
    for (const auto& pair : new_params_) {
      auto res = raw_params_.insert_or_assign(pair.first, pair.second);
      // If did an insertion instead of an assignment
      if (res.second) {
        std::ostringstream ss;
        ss << "Unknown param for current workload: " << pair.first << ". Default params: " << ToString();
        throw std::runtime_error(ss.str());
      }
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
        throw std::runtime_error("Invalid workload param token: " + token);
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

  struct Record {
    uint32_t partition;
    uint32_t home;
    bool is_hot;
    bool is_write;
  };

  std::map<Key, Record> records;
};

/**
 * Base class for a workload generator
 */
class Workload {
 public:
  Workload(const RawParamMap& default_params, const std::string& params_str) : params_(default_params) {
    params_.Update(params_str);
  }

  /**
   * Gets the next transaction in the workload
   */
  virtual std::pair<Transaction*, TransactionProfile> NextTransaction() = 0;

  std::string GetParamsStr() { return params_.ToString(); }

  static const RawParamMap MergeParams(const RawParamMap& p1, const RawParamMap& p2) {
    RawParamMap params;
    params.insert(p1.begin(), p1.end());
    params.insert(p2.begin(), p2.end());
    return params;
  }

 protected:
  WorkloadParams params_;
};

const std::string kCharacters("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz");
/**
 * Generates a random string of length n
 */
inline std::string RandomString(size_t n, std::mt19937& rg) {
  std::uniform_int_distribution<uint32_t> dis(0, kCharacters.size() - 1);
  std::string s;
  s.reserve(n);
  for (size_t i = 0; i < n; i++) {
    s += kCharacters[dis(rg)];
  }
  return s;
}

class KeyList {
 public:
  KeyList(size_t num_hot_keys = 0) : is_simple_(false), num_hot_keys_(num_hot_keys) {}

  KeyList(const ConfigurationPtr config, int partition, int master, size_t num_hot_keys = 0)
      : is_simple_(true), partition_(partition), master_(master), num_hot_keys_(num_hot_keys) {
    auto simple_partitioning = config->simple_partitioning();
    auto num_records = static_cast<long long>(simple_partitioning->num_records());
    num_partitions_ = config->num_partitions();
    num_replicas_ = config->num_replicas();
    num_keys_ = std::max(1LL, ((num_records - partition) / num_partitions_ - master) / num_replicas_);
  }

  void AddKey(Key key) {
    if (is_simple_) {
      throw std::runtime_error("Cannot add keys to a simple key list");
    }
    if (static_cast<long long>(hot_keys_.size()) < num_hot_keys_) {
      hot_keys_.push_back(key);
      return;
    }
    cold_keys_.push_back(key);
  }

  Key GetRandomHotKey(std::mt19937& rg) {
    if (num_hot_keys_ == 0) {
      throw std::runtime_error("There is no hot key to pick from. Please check your params.");
    }
    if (is_simple_) {
      std::uniform_int_distribution<uint64_t> dis(0, std::min(num_hot_keys_, num_keys_) - 1);
      uint64_t key = num_partitions_ * (dis(rg) * num_replicas_ + master_) + partition_;
      return std::to_string(key);
    }
    std::uniform_int_distribution<uint32_t> dis(0, hot_keys_.size() - 1);
    return hot_keys_[dis(rg)];
  }

  Key GetRandomColdKey(std::mt19937& rg) {
    if (is_simple_) {
      if (num_hot_keys_ >= num_keys_) {
        throw std::runtime_error("There is no cold key to pick from. Please check your params.");
      }
      std::uniform_int_distribution<uint64_t> dis(num_hot_keys_, num_keys_ - 1);
      uint64_t key = num_partitions_ * (dis(rg) * num_replicas_ + master_) + partition_;
      return std::to_string(key);
    }
    if (cold_keys_.empty()) {
      throw std::runtime_error("There is no cold key to pick from. Please check your params.");
    }
    std::uniform_int_distribution<uint32_t> dis(0, cold_keys_.size() - 1);
    return cold_keys_[dis(rg)];
  }

 private:
  bool is_simple_;
  int partition_;
  int master_;
  int num_partitions_;
  int num_replicas_;
  long long num_keys_;
  long long num_hot_keys_;
  std::vector<Key> cold_keys_;
  std::vector<Key> hot_keys_;
};

}  // namespace slog