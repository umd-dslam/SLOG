#pragma once

#include <glog/logging.h>

#include <optional>
#include <vector>

#include "common/configuration.h"
#include "common/types.h"
#include "proto/transaction.pb.h"

namespace slog {

using EnvelopePtr = std::unique_ptr<internal::Envelope>;

class TxnHolder;

struct LockOnlyTxn {
  LockOnlyTxn(TxnHolder& holder, uint32_t master) : holder(holder), master(master) {}

  TxnHolder& holder;

  struct KeyInfo {
    KeyInfo(const Key& key, LockMode mode, uint32_t counter) : key(key), mode(mode), counter(counter) {}
    bool operator==(const KeyInfo& other) const {
      return key == other.key && mode == other.mode && counter == other.counter;
    }
    const Key key;
    const LockMode mode;
    const uint32_t counter;
  };

  std::vector<KeyInfo> keys;
  const uint32_t master;
};

class TxnHolder {
 public:
  TxnHolder(const ConfigurationPtr& config, Transaction* txn)
      : txn_(txn),
        aborting_(false),
        done_(false),
        lo_txns_(config->num_replicas(), std::nullopt),
        any_lo_txn_(nullptr),
        num_lo_txns_(0) {
    keys_in_partition_.clear();
    active_partitions_.clear();
    for (const auto& kv : txn_->read_set()) {
      if (config->key_is_in_local_partition(kv.first) && !txn_->write_set().contains(kv.first)) {
        keys_in_partition_.emplace_back(kv.first, LockMode::READ);
      }
    }
    for (const auto& kv : txn_->write_set()) {
      active_partitions_.push_back(config->partition_of_key(kv.first));
      if (config->key_is_in_local_partition(kv.first)) {
        keys_in_partition_.emplace_back(kv.first, LockMode::WRITE);
      }
    }
    std::sort(active_partitions_.begin(), active_partitions_.end());
    auto last = std::unique(active_partitions_.begin(), active_partitions_.end());
    active_partitions_.erase(last, active_partitions_.end());

    AddLockOnlyTxn(txn);
  }

  ~TxnHolder() { delete txn_; }

  const LockOnlyTxn* AddLockOnlyTxn(Transaction* new_txn) {
    auto rep_id = replica_id(new_txn);

#ifdef REMASTER_PROTOCOL_COUNTERLESS
    if (new_txn->procedure_case() == Transaction::kRemaster && new_txn->remaster().is_new_master_lock_only()) {
      rep_id = new_txn->remaster().new_master();
    }
#endif

    if (lo_txns_[rep_id].has_value()) {
      LOG(WARNING) << "Lock-only txn already created for rep_id = " << rep_id;
      return nullptr;
    }

    auto& lo_txn = lo_txns_[rep_id].emplace(*this, rep_id);
    auto& new_metadata = new_txn->internal().master_metadata();
    for (const auto& km : keys_in_partition_) {
      auto it = new_metadata.find(km.first);
      if (it != new_metadata.end()) {
        lo_txn.keys.emplace_back(km.first, km.second, it->second.counter());
      }
    }

    txn_->mutable_internal()->mutable_master_metadata()->insert(new_metadata.begin(), new_metadata.end());

    ++num_lo_txns_;

    if (num_lo_txns_ == 1) {
      any_lo_txn_ = &lo_txn;
    } else {
      delete new_txn;
    }

    return &lo_txn;
  }

  Transaction* txn() const { return txn_; }
  TxnId id() const { return txn_->internal().id(); }

  const std::vector<std::pair<Key, LockMode>>& keys_in_partition() const { return keys_in_partition_; }

  const std::vector<uint32_t>& active_partitions() const { return active_partitions_; }

  const std::vector<std::optional<LockOnlyTxn>>& lock_only_txns() const { return lo_txns_; }
  const LockOnlyTxn& lock_only_txn(size_t i) const { return lo_txns_[i].value(); }
  LockOnlyTxn* any_lock_only_txn() const { return any_lo_txn_; }

  bool is_ready_for_gc() const { return done_ && num_lo_txns_ == txn_->internal().involved_replicas_size(); }

  /**
   * Get the id of the replica where this transaction was added to the local log.
   * Should only be used for single-home and lock-only transactions.
   */
  uint32_t replica_id() const { return replica_id(txn_); }

  static uint32_t replica_id(const Transaction* txn) {
    // This should only be empty for testing.
    // TODO: add metadata to test cases, make this an error
    //
    // Note that this uses all metadata, not just keys in partition. It's therefore safe
    // to call this on transactions that don't involve the current partition
    if (txn->internal().master_metadata().empty()) {
      LOG(WARNING) << "Master metadata empty: txn id " << txn->internal().id();
      return 0;
    }

#ifdef REMASTER_PROTOCOL_COUNTERLESS
    if (txn->procedure_case() == Transaction::kRemaster && txn->remaster().is_new_master_lock_only()) {
      return txn->remaster().new_master();
    }
#endif

    // Get the master of an element from the metadata. For single-home and lock-onlies, all masters
    // will be the same in the metadata
    return txn->internal().master_metadata().begin()->second.master();
  }

  void SetDone() { done_ = true; }
  bool is_done() const { return done_; }
  void SetAborting() { aborting_ = true; }
  bool is_aborting() const { return aborting_; }

 private:
  Transaction* txn_;
  bool aborting_;
  bool done_;

  std::vector<std::optional<LockOnlyTxn>> lo_txns_;
  LockOnlyTxn* any_lo_txn_;
  int num_lo_txns_;
  std::vector<std::pair<Key, LockMode>> keys_in_partition_;
  std::vector<uint32_t> active_partitions_;
};

}  // namespace slog