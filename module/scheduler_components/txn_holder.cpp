#include "module/scheduler_components/txn_holder.h"

namespace slog {

TxnHolder::TxnHolder(const ConfigurationPtr& config, Transaction* txn)
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

bool TxnHolder::AddLockOnlyTxn(Transaction* txn) {
  auto home = txn->internal().home();
  CHECK_LT(home, static_cast<int>(lo_txns_.size()));

  if (lo_txns_[home] != nullptr) {
    return false;
  }

  lo_txns_[home].reset(txn);

  ++num_lo_txns_;

  return true;
}

Transaction* TxnHolder::FinalizeAndRelease() {
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
      for (int i = cutoff; i < internal->events_size(); i++) {
        main_internal->mutable_events()->AddAllocated(internal->mutable_events(i));
      }
      while (internal->events_size() > cutoff) {
        internal->mutable_events()->ReleaseLast();
      }
      lo_txn.reset();
    }
  }

  // Do not use clear() here because lo_txns_ must never change in size
  return lo_txns_[main_txn_idx_].release();
}

}  // namespace slog