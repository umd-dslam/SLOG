#pragma once

#include <unordered_map>
#include <random>

#include "common/types.h"
#include "proto/transaction.pb.h"

namespace slog {

const uint32_t MP_NUM_PARTITIONS = 2;
const uint32_t MH_NUM_HOMES = 2;
const uint32_t NUM_RECORDS = 10;
const uint32_t NUM_WRITES = 2;
const uint32_t VALUE_SIZE = 100; // bytes

struct TransactionProfile {
  TxnId client_txn_id;
  bool is_multi_home;
  bool is_multi_partition;

  std::unordered_map<Key, uint32_t> key_to_partition;
  std::unordered_map<Key, uint32_t> key_to_home;
};

/**
 * Base class for a workload generator
 */
class WorkloadGenerator {
public:
  /**
   * Gets the next transaction in the workload
   */
  virtual std::pair<Transaction*, TransactionProfile> NextTransaction() = 0;
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

} // namespace slog