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
  TxnHolder(const ConfigurationPtr& config, Transaction* txn);

  bool AddLockOnlyTxn(Transaction* txn);

  Transaction* FinalizeAndRelease();

  TxnId txn_id() const { return txn_id_; }
  Transaction& txn() const {
    CHECK(lo_txns_[main_txn_idx_] != nullptr);
    return *lo_txns_[main_txn_idx_];
  }
  Transaction& lock_only_txn(size_t i) const { return *lo_txns_[i]; }

  void SetRemasterResult(const Key& key, uint32_t counter) { remaster_result_.emplace(key, counter); }
  std::optional<pair<Key, uint32_t>> remaster_result() const { return remaster_result_; }

  void SetDone() { done_ = true; }
  bool is_done() const { return done_; }

  void SetAborting() { aborting_ = true; }
  bool is_aborting() const { return aborting_; }

  void IncNumDispatches() { num_dispatches_++; }
  int num_dispatches() const { return num_dispatches_; }

  bool is_ready_for_gc() const { return done_ && num_lo_txns_ == expected_num_lo_txns_; }
  int num_lock_only_txns() const { return num_lo_txns_; }
  int expected_num_lock_only_txns() const { return expected_num_lo_txns_; }

 private:
  TxnId txn_id_;
  size_t main_txn_idx_;
  std::vector<std::unique_ptr<Transaction>> lo_txns_;
  std::optional<pair<Key, uint32_t>> remaster_result_;
  bool aborting_;
  bool done_;
  int num_lo_txns_;
  int expected_num_lo_txns_;
  int num_dispatches_;
};

}  // namespace slog