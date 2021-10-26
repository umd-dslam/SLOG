#include "workload/cockroach.h"

#include <fcntl.h>
#include <glog/logging.h>

#include <algorithm>
#include <random>
#include <sstream>
#include <unordered_set>

#include "common/proto_utils.h"

using std::bernoulli_distribution;
using std::vector;

namespace slog {
namespace {

// Percentage of multi-home transactions
constexpr char MH_PCT[] = "mh";
// Theta parameter of the zipf distribution
// See "Quickly Generating Billion-Record Synthetic Databases"
// by Gray, Sundaresan, Englert, Baclawski, and Weinberger, SIGMOD 1994.
constexpr char THETA[] = "theta";
// Number of records in a transaction
constexpr char RECORDS[] = "records";
// Number of write records in a transaction
constexpr char WRITES[] = "writes";
// Size of a written value in bytes
constexpr char VALUE_SIZE[] = "value_size";

const RawParamMap DEFAULT_PARAMS = {{MH_PCT, "0"}, {THETA, "0.1"}, {RECORDS, "10"}, {WRITES, "10"},  {VALUE_SIZE, "50"}};

long long NumKeysPerRegion(const ConfigurationPtr& config) {
  auto simple_partitioning = config->proto_config().simple_partitioning();
  auto num_records = static_cast<long long>(simple_partitioning.num_records());
  return num_records / config->num_replicas();
}

}  // namespace

CockroachWorkload::CockroachWorkload(const ConfigurationPtr& config, uint32_t region,
                                     const string& params_str, const uint32_t seed)
    : Workload(DEFAULT_PARAMS, params_str),
      config_(config),
      local_region_(region),
      zipf_(0, NumKeysPerRegion(config) - 1, params_.GetDouble(THETA)),
      rg_(seed),
      rnd_str_(seed),
      client_txn_id_counter_(0) {
  name_ = "cockroach";
}

std::pair<Transaction*, TransactionProfile> CockroachWorkload::NextTransaction() {
  TransactionProfile pro;

  pro.client_txn_id = client_txn_id_counter_;

  auto num_replicas = config_->num_replicas();
  auto num_partitions = config_->num_partitions();

  // Decide if this is a multi-home txn or not
  auto multi_home_pct = params_.GetDouble(MH_PCT);
  bernoulli_distribution is_mh(multi_home_pct / 100);
  pro.is_multi_home = is_mh(rg_);

  auto writes = params_.GetUInt32(WRITES);
  auto records = params_.GetUInt32(RECORDS);
  auto value_size = params_.GetUInt32(VALUE_SIZE);

  CHECK_LE(writes, records) << "Number of writes cannot exceed number of records in a transaction!";

  auto local_records = pro.is_multi_home ? records / 2 : records;
  vector<uint64_t> numeric_keys(records);

  auto local_keys = KeyBatch(local_records);
  for (size_t i = 0; i < local_records; i++) {
    numeric_keys[i] = local_keys[i] * num_replicas + local_region_;
  }

  // Select a remote region
  std::uniform_int_distribution<> dis(1, num_replicas - 1);
  uint32_t remote_region = local_region_ + dis(rg_);

  auto remote_keys = KeyBatch(records - local_records);
  for (size_t i = local_records; i < records; i++) {
    numeric_keys[i] = remote_keys[i - local_records] * num_replicas + remote_region;
  }

  vector<KeyMetadata> keys;
  vector<vector<string>> code;

  int last_partition = -1;
  for (size_t i = 0; i < numeric_keys.size(); i++) {
    Key key = std::to_string(numeric_keys[i]);
    auto ins = pro.records.emplace(key, TransactionProfile::Record());
    auto &record = ins.first->second;
    if (i < writes) {
      code.push_back({"SET", key, rnd_str_(value_size)});
      keys.emplace_back(key, KeyType::WRITE);
      record.is_write = true;
    } else {
      code.push_back({"GET", key});
      keys.emplace_back(key, KeyType::READ);
      record.is_write = false;
    }
    record.home = i < local_records ? local_region_ : remote_region;
    record.partition = numeric_keys[i] % num_partitions;
    record.is_hot = i == 0 || i == local_records;
    if (last_partition == -1) {
      last_partition = record.partition;
    } else if (record.partition != static_cast<uint32_t>(last_partition)) {
      pro.is_multi_partition = true;
    }
  }

  // Construct a new transaction
  auto txn = MakeTransaction(keys, code);
  txn->mutable_internal()->set_id(client_txn_id_counter_);

  client_txn_id_counter_++;

  return {txn, pro};
}

std::vector<uint64_t> CockroachWorkload::KeyBatch(int n) {
  if (n == 0) {
    return {};
  }
  std::unordered_set<uint64_t> exists;
  std::vector<uint64_t> res(n);
  res[0] = zipf_(rg_);
  exists.insert(res[0]);
  std::uniform_int_distribution<> u(0, zipf_.Max());
  for (int i = 1; i < n; i++) {
    for (;;) {
      res[i] = u(rg_);
      if (exists.find(res[i]) == exists.end()) {
        break;
      }
    }
    exists.insert(res[i]);
  }
  return res;
}

}  // namespace slog