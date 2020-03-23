#include "paxos/quorum_tracker.h"

namespace slog {

using internal::Response;

QuorumTracker::QuorumTracker(uint32_t num_members)
  : num_members_(num_members),
    state_(QuorumState::INCOMPLETE) {}

bool QuorumTracker::HandleResponse(
    const Response& res,
    const string& from_machine_id) {
  if (state_ == QuorumState::COMPLETE || state_ == QuorumState::ABORTED) {
    return false;
  }
  if (!ResponseIsValid(res)) {
    return false;
  }

  machine_responded_.insert(from_machine_id);

  auto sz = machine_responded_.size();
  if (sz == num_members_) {
    state_ = QuorumState::COMPLETE;
    return true;
  }
  // Check whether the current state is already QUORUM_REACHED so that
  // we only report state change once
  if (sz > num_members_ / 2 && state_ != QuorumState::QUORUM_REACHED) {
    state_ = QuorumState::QUORUM_REACHED;
    return true;
  } 
  return false;
}

void QuorumTracker::Abort() {
  state_ = QuorumState::ABORTED;
}

QuorumState QuorumTracker::GetState() const {
  return state_;
}

AcceptanceTracker::AcceptanceTracker(
    uint32_t num_members,
    uint32_t ballot,
    uint32_t slot)
  : QuorumTracker(num_members),
    ballot(ballot),
    slot(slot) {}

bool AcceptanceTracker::ResponseIsValid(const internal::Response& res) {
  if (res.type_case() != Response::TypeCase::kPaxosAccept) {
    return false;
  }
  auto paxos_accept = res.paxos_accept();
  return paxos_accept.ballot() == ballot && paxos_accept.slot() == slot;
}

CommitTracker::CommitTracker(
    uint32_t num_members,
    uint32_t slot)
  : QuorumTracker(num_members),
    slot(slot) {}

bool CommitTracker::ResponseIsValid(const internal::Response& res) {
  if (res.type_case() != Response::TypeCase::kPaxosCommit) {
    return false;
  }
  auto paxos_commit = res.paxos_commit();
  return paxos_commit.slot() == slot;
}

} // namespace slog