#pragma once

#include <glog/logging.h>

#include <optional>
#include <vector>

#include "common/configuration.h"
#include "common/proto_utils.h"
#include "common/types.h"
#include "proto/transaction.pb.h"

namespace slog {

using EnvelopePtr = std::unique_ptr<internal::Envelope>;

class TxnHolder {
 public:
  TxnHolder(const ConfigurationPtr& config, Transaction* txn)
      : id_(txn->internal().id()),
        main_txn_(txn->internal().home()),
        lo_txns_(config->num_replicas()),
        remaster_result_(std::nullopt),
        aborting_(false),
        done_(false),
        num_lo_txns_(0),
        expected_num_lo_txns_(txn->internal().involved_replicas_size()) {
    lo_txns_[main_txn_].reset(txn);
    ++num_lo_txns_;
  }

  bool AddLockOnlyTxn(Transaction* txn) {
    auto home = txn->internal().home();
    if (home >= static_cast<int>(lo_txns_.size()) || lo_txns_[home] != nullptr) {
      return false;
    }
    lo_txns_[home].reset(txn);
    ++num_lo_txns_;
    return true;
  }

  Transaction* Release() {
    auto txn = lo_txns_[main_txn_].release();
    lo_txns_.clear();
    return txn;
  }

  TxnId id() const { return id_; }
  Transaction& txn() const { return *lo_txns_[main_txn_]; }
  Transaction& lock_only_txn(size_t i) const { return *lo_txns_[i]; }

  void SetRemasterResult(const Key& key, uint32_t counter) { remaster_result_.emplace(key, counter); }
  std::optional<pair<Key, uint32_t>> remaster_result() const { return remaster_result_; }

  void SetDone() { done_ = true; }
  bool is_done() const { return done_; }

  void SetAborting() { aborting_ = true; }
  bool is_aborting() const { return aborting_; }

  bool is_ready_for_gc() const { return done_ && num_lo_txns_ == expected_num_lo_txns_; }
  int num_lock_only_txns() const { return num_lo_txns_; }
  int expected_num_lock_only_txns() const { return expected_num_lo_txns_; }

 private:
  TxnId id_;
  size_t main_txn_;
  std::vector<std::unique_ptr<Transaction>> lo_txns_;
  std::optional<pair<Key, uint32_t>> remaster_result_;
  bool aborting_;
  bool done_;
  int num_lo_txns_;
  int expected_num_lo_txns_;
};

}  // namespace slog