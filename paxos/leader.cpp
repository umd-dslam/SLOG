#include "paxos/leader.h"
#include "paxos/simple_multi_paxos.h"

namespace slog {

using internal::Request;
using internal::Response;

Leader::Leader(
    SimpleMultiPaxos& sender,
    const vector<string>& members,
    const string& me)
  : sender_(sender), 
    members_(members),
    me_(me),
    min_uncommitted_slot_(0),
    next_empty_slot_(0) {
  auto it = std::find(members.begin(), members.end(), me);
  is_member_ = it != members.end();
  if (is_member_) {
    auto position_in_members = it - members.begin();
    is_elected_ = position_in_members == PAXOS_DEFAULT_LEADER_POSITION;
    ballot_ = position_in_members;
  } else {
    // When the current machine is not a member of this paxos group, it
    // will always forward a proposal request to the initially elected
    // leader of the group (which would never change in this implementation 
    // of paxos)
    is_elected_ = false;
  }
  elected_leader_ = members[PAXOS_DEFAULT_LEADER_POSITION];
}

void Leader::HandleRequest(const Request& req) {
  switch (req.type_case()) {
    case Request::TypeCase::kPaxosPropose:
      // If elected as true leader, send accept request to the acceptors
      // Otherwise, forward the request to the true leader
      if (is_elected_) {
        StartNewAcceptance(req.paxos_propose().value());
      } else {
        sender_.SendSameChannel(req, elected_leader_);
      }
      break;
    case Request::TypeCase::kPaxosCommit:
      ProcessCommitRequest(req.paxos_commit()); break;
    default:
      break;
  }
}

void Leader::ProcessCommitRequest(const internal::PaxosCommitRequest commit) {
  auto ballot = commit.ballot();
  auto slot = commit.slot();
  auto value = commit.value();

  if (slot < min_uncommitted_slot_) {
    // Ignore committed messages. We already forget about values of these older
    // slots so we cannot check for paxos invariant like we do below
    return;
  }

  auto& proposal = proposals_[slot];
  if (proposal.is_committed) {
    CHECK_EQ(value, proposal.value) 
        << "Paxos invariant violated: Two values are committed for the same slot";
    CHECK_EQ(ballot, proposal.ballot)
        << "Paxos invariatn violated: Two leaders commit to the same slot";
  }
  proposal.ballot = ballot;
  proposal.value = value;
  proposal.is_committed = true;

  // Report to the paxos user
  sender_.OnCommit(slot, value);

  if (slot >= next_empty_slot_) {
    next_empty_slot_ = slot + 1;
  }
  while (proposals_.count(min_uncommitted_slot_) > 0 
      && proposals_[min_uncommitted_slot_].is_committed) {
    proposals_.erase(min_uncommitted_slot_);
    min_uncommitted_slot_++;
  }
}

void Leader::HandleResponse(
    const Response& res,
    const string& from_machine_id) {
  for (auto& tracker : quorum_trackers_) {
    bool state_changed = tracker->HandleResponse(res, from_machine_id);
    if (state_changed) {
      const auto raw_tracker = tracker.get();

      if (const auto acceptance = dynamic_cast<AcceptanceTracker*>(raw_tracker)) {
        AcceptanceStateChanged(acceptance);
      }
      else if (const auto commit = dynamic_cast<CommitTracker*>(raw_tracker)) {
        CommitStateChanged(commit);
      }
    }
  }
  // Clean up trackers with COMPLETE and ABORTED state
  auto pend = std::remove_if(
      quorum_trackers_.begin(),
      quorum_trackers_.end(),
      [](auto& tracker) {
        return tracker->GetState() == QuorumState::COMPLETE 
            || tracker->GetState() == QuorumState::ABORTED;});
  quorum_trackers_.erase(pend, quorum_trackers_.end());
}

void Leader::StartNewAcceptance(uint32_t value) {
  proposals_[next_empty_slot_] = Proposal(ballot_, value);
  quorum_trackers_.emplace_back(
      new AcceptanceTracker(members_.size(), ballot_, next_empty_slot_));

  Request request;
  auto paxos_accept = request.mutable_paxos_accept();
  paxos_accept->set_ballot(ballot_);
  paxos_accept->set_slot(next_empty_slot_);
  paxos_accept->set_value(value);
  next_empty_slot_++;

  SendToAllMembers(request);
}

void Leader::AcceptanceStateChanged(AcceptanceTracker* acceptance) {
  // The check for member size is an optimization. Without this check, we'll send commit
  // messages twice: once when quorum is reached and once when all acceptance messages are
  // received. When member size <= 2, the state COMPLETE would overshadow QUORUM_REACHED,
  // so we only need to check for COMPLETE in this case. Otherwise, QUORUM_REACHED is enough.
  if (acceptance->GetState() == QuorumState::QUORUM_REACHED
      || (members_.size() <= 2 && acceptance->GetState() == QuorumState::COMPLETE)) {
    auto slot = acceptance->slot;
    StartNewCommit(slot);
  }
  // TODO: Retransmit request after we implement heartbeat
}

void Leader::StartNewCommit(SlotId slot) {
  quorum_trackers_.emplace_back(
      new CommitTracker(members_.size(), slot));
  
  Request request;
  auto paxos_commit = request.mutable_paxos_commit();
  paxos_commit->set_slot(slot);
  paxos_commit->set_value(proposals_[slot].value);

  SendToAllMembers(request);
}

void Leader::CommitStateChanged(CommitTracker* /* commit */) {
  // TODO: Retransmit request after we implement heartbeat
}

void Leader::SendToAllMembers(const Request& request) {
  for (const auto& member : members_) {
    sender_.SendSameChannel(request, member);
  }
}

bool Leader::IsMember() const {
  return is_member_;
}

} // namespace slog