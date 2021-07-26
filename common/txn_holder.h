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
      : txn_id_(txn->internal().id()),
        main_txn_idx_(txn->internal().home()),
        lo_txns_(config->num_replicas()),
        remaster_result_(std::nullopt),
        aborting_(false),
        done_(false),
        num_lo_txns_(0),
        expected_num_lo_txns_(txn->internal().involved_replicas_size()),
        num_dispatches_(0) {
    lo_txns_[main_txn_idx_].reset(txn);
    ++num_lo_txns_;
  }

  bool AddLockOnlyTxn(Transaction* txn) {
    auto home = txn->internal().home();
    CHECK_LT(home, static_cast<int>(lo_txns_.size()));

    if (lo_txns_[home] != nullptr) {
      return false;
    }

    lo_txns_[home].reset(txn);

    ++num_lo_txns_;

    return true;
  }

  Transaction* FinalizeAndRelease() {
    auto& main_txn = lo_txns_[main_txn_idx_];
    auto main_internal = main_txn->mutable_internal();
    int cutoff = main_internal->events_size();
    for (auto& lo_txn : lo_txns_) {
      if (lo_txn != nullptr) {
        auto internal = lo_txn->mutable_internal();

        // Set the "home" field of each event and determine the cutoff point
        // before which the current txn shares the same events as the main txn
        for (int i = 0; i < internal->events_size(); i++) {
          auto event = internal->mutable_events(i);
          event->set_home(internal->home());

          if (i < cutoff) {
            const auto& main_txn_event = main_internal->events(i);
            if (event->event() != main_txn_event.event() || event->time() != main_txn_event.time() ||
                event->machine() != main_txn_event.machine()) {
              cutoff = i;
            }
          }
        }
      }
    }

    for (auto& lo_txn : lo_txns_) {
      if (lo_txn != nullptr && lo_txn != main_txn) {
        auto internal = lo_txn->mutable_internal();
        // Only transfer the events after the cutoff point to the main txn
        while (internal->events_size() > cutoff) {
          main_internal->mutable_events()->AddAllocated(internal->mutable_events()->ReleaseLast());
        }
        lo_txn.reset();
      }
    }

    // Do not use clear() here because lo_txns_ must never change in size
    return lo_txns_[main_txn_idx_].release();
  }

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