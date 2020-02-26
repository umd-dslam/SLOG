#include "benchmark/basic_workload.h"

#include <algorithm>
#include <iomanip>
#include <fcntl.h>
#include <random>
#include <sstream>
#include <unordered_set>
#include <vector>

#include <glog/logging.h>

#include "common/offline_data_reader.h"
#include "common/proto_utils.h"
#include "proto/offline_data.pb.h"

using std::discrete_distribution;
using std::shuffle;
using std::uniform_int_distribution;
using std::string;
using std::vector;
using std::unordered_set;

namespace slog {

namespace {

/**
 * Chooses without replacement k elements from [0, n)
 */
vector<uint32_t> Choose(uint32_t n, uint32_t k, std::mt19937& re) {
  if (n == 0) {
    return {};
  }
  if (k == 1) {
    // For k = 1, it is faster to pick a random key than shuffling
    // the whole vector and pick the first key.
    uniform_int_distribution<uint32_t> dis(0, n - 1);
    return {dis(re)};
  }
  vector<uint32_t> a(n);
  std::iota(a.begin(), a.end(), 0);
  shuffle(a.begin(), a.end(), re);
  return {a.begin(), a.begin() + std::min(n, k)};
}

/**
 * Randomly picks an element from a vector uniformly
 */
template<typename T>
T PickOne(const vector<T>& v, std::mt19937& re) {
  auto i = Choose(v.size(), 1, re)[0];
  return v[i];
}

const std::string CHARACTERS("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz");

/**
 * Generates a random string of length n
 */
string RandomString(size_t n, std::mt19937& re) {
  string s;
  for (size_t i = 0; i < n; i++) {
    auto k = Choose(CHARACTERS.size(), 1, re)[0];
    s += CHARACTERS[k];
  }
  return s;
}

} // namespace

KeyList::KeyList(size_t num_hot_keys) : num_hot_keys_(num_hot_keys) {}

void KeyList::AddKey(Key key) {
  if (hot_keys_.size() < num_hot_keys_) {
    hot_keys_.push_back(key);
    return;
  }
  cold_keys_.push_back(key);
}

Key KeyList::GetRandomHotKey() {
  if (hot_keys_.empty()) {
    throw std::runtime_error("There is no hot key to pick from. Please check your data");
  }
  return PickOne(hot_keys_, re_);
}

Key KeyList::GetRandomColdKey() {
  if (cold_keys_.empty()) {
    throw std::runtime_error("There is no cold key to pick from. Please check your data");
  }
  return PickOne(cold_keys_, re_);
}

BasicWorkload::BasicWorkload(
    ConfigurationPtr config,
    std::string data_dir,
    double multi_home_pct,
    double multi_partition_pct)
  : config_(config),
    multi_home_pct_(multi_home_pct),
    multi_partition_pct_(multi_partition_pct),
    partition_to_key_lists_(config->GetNumPartitions()) {

  for (auto& key_lists : partition_to_key_lists_) {
    for (uint32_t rep = 0; rep < config->GetNumReplicas(); rep++) {
      // TODO (ctring): initialize each key list with some hot keys
      key_lists.emplace_back();
    }
  }

  // Load and index the initial data
  for (uint32_t partition = 0; partition < config->GetNumPartitions(); partition++) {
    auto data_file = data_dir + "/" + std::to_string(partition) + ".dat";
    auto fd = open(data_file.c_str(), O_RDONLY);
    if (fd < 0) {
      LOG(FATAL) << "Error while loading \"" << data_file << "\": " << strerror(errno);
    }

    OfflineDataReader reader(fd);
    LOG(INFO) << "Loading " << reader.GetNumDatums() << " datums from " << data_file;
    while (reader.HasNextDatum()) {
      auto datum = reader.GetNextDatum();
      CHECK_LT(datum.master(), config->GetNumReplicas())
          << "Datum is mastered at a non-existent replica: " << datum.master()
          << ". Number of replicas from config: " << config->GetNumReplicas();

      partition_to_key_lists_[partition][datum.master()].AddKey(datum.key());
    }
    close(fd);
  }
}

Transaction BasicWorkload::NextTransaction() {
  CHECK_LE(NUM_WRITES, NUM_RECORDS) 
      << "Number of writes cannot exceed number of records in a txn!";

  // Decide if this is a multi-partition txn or not
  discrete_distribution<> spmp({100 - multi_partition_pct_, multi_partition_pct_});
  bool is_multi_partition = spmp(re_);

  // Select a number of partitions to choose from for each record
  auto candidate_partitions = Choose(
      config_->GetNumPartitions(),
      is_multi_partition ? MP_NUM_PARTITIONS : 1,
      re_);

  // Decide if this is a multi-home txn or not
  discrete_distribution<> shmh({100 - multi_home_pct_, multi_home_pct_});
  bool is_multi_home = shmh(re_);
  
  // Select a number of homes to choose from for each record
  auto candidate_homes = Choose(
      config_->GetNumReplicas(),
      is_multi_home ? MH_NUM_HOMES : 1,
      re_);

  unordered_set<Key> read_set;
  unordered_set<Key> write_set;
  std::ostringstream code;

  // For logging
  vector<pair<uint32_t, uint32_t>> picked_sources;

  // Fill txn with read operators
  for (size_t i = 0; i < NUM_RECORDS - NUM_WRITES; i++) {
    auto partition = PickOne(candidate_partitions, re_);
    auto home = PickOne(candidate_homes, re_);
    // TODO: Add hot keys later
    auto key = partition_to_key_lists_[partition][home].GetRandomColdKey();
    code << "GET " << key << " ";
    read_set.insert(key);

    picked_sources.emplace_back(partition, home);
  }

  // Fill txn with write operators
  for (size_t i = 0; i < NUM_WRITES; i++) {
    auto partition = PickOne(candidate_partitions, re_);
    auto home = PickOne(candidate_homes, re_);
    // TODO: Add hot keys later
    auto key = partition_to_key_lists_[partition][home].GetRandomColdKey();
    code << "SET " << key << " " << RandomString(VALUE_SIZE, re_) << " ";
    write_set.insert(key);

    picked_sources.emplace_back(partition, home);
  }

  std::ostringstream log;
  log << "partition: ";
  for (auto& src : picked_sources) {
    log << std::setw(2) << src.first << " ";
  }
  log << std::endl << std::setw(11) << "home: ";
  for (auto& src : picked_sources) {
    log << std::setw(2) << src.second << " ";
  }

  VLOG(1) << "Source of keys:\n" << log.str();

  return MakeTransaction(read_set, write_set, code.str());
}

} // namespace slog