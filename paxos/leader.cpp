#include "paxos/leader.h"
#include "paxos/simple_multi_paxos.h"

namespace slog {

using internal::Request;
using internal::Response;

Leader::Leader(
    SimpleMultiPaxos* sender,
    const vector<string>& members,
    const string& me)
  : sender_(sender), 
    members_(members),
    me_(me),
    min_uncommitted_slot_(0),
    next_empty_slot_(0) {
  auto position_in_members = 
      std::find(members.begin(), members.end(), me) - members.begin();
  is_elected_ = position_in_members == 0;
  elected_leader_ = members[0];
  ballot_ = position_in_members;
}

void Leader::HandleRequest(const Request& req) {
  switch (req.type_case()) {
    case Request::TypeCase::kPaxosPropose:
      // If elected as true leader, send accept request to the acceptors
      // Otherwise, forward the request to the true leader
      if (is_elected_) {
        StartNewAcceptance(req.paxos_propose().value());
      } else {
        sender_->SendSameChannel(req, elected_leader_);
      }
      break;
    case Request::TypeCase::kPaxosCommit:
      HandleCommitRequest(req.paxos_commit()); break;
    default:
      break;
  }
}

void Leader::HandleCommitRequest(const internal::PaxosCommitRequest commit) {
  auto ballot = commit.ballot();
  auto slot = commit.slot();
  auto value = commit.value();
  auto& proposal = proposals_[slot];
  
  if (proposal.is_chosen) {
    CHECK_EQ(value, proposal.value) 
        << "Paxos invariant violated: Two values are chosen for the same slot";
  }
  proposal.ballot = ballot;
  proposal.value = value;
  proposal.is_chosen = true;

  if (slot >= next_empty_slot_) {
    next_empty_slot_ = slot + 1;
  }

  while (proposals_.count(min_uncommitted_slot_) > 0 
      && proposals_[min_uncommitted_slot_].is_chosen) {
    auto value = proposals_[min_uncommitted_slot_].value;

    sender_->OnCommit(min_uncommitted_slot_, value);

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

      // TODO: Current assumption is that the machines won't fail so this is not neccessary. 
      //       Continue working on this after we change the assumption
      // if (const auto election = dynamic_cast<ElectionTracker*>(raw_tracker)) {
      //   ElectionStateChanged(election);
      // }
    }
    // Clean up trackers with COMPLETE and ABORTED state
    if (tracker->GetState() == QuorumState::COMPLETE
        || tracker->GetState() == QuorumState::ABORTED) {
      quorum_trackers_.erase(tracker);
    }
  }
  
}

// TODO: Current assumption is that the machines won't fail so this is not neccessary. 
//       Continue working on this after we change the assumption
/*
void Leader::AdvanceBallot() {
  ballot_ += members_.size();
}

void Leader::StartNewElection() {
  quorum_trackers_.emplace(
      new ElectionTracker(members_.size(), ballot_));

  Request request;
  auto paxos_elect = request.mutable_paxos_elect();
  paxos_elect->set_ballot(ballot_);
  for (uint32_t i = min_uncommitted_slot_; i < next_empty_slot_; i++) {
    if (proposals_.count(i) == 0) {
      paxos_elect->add_gaps(i);
    }
  }
  SendToAllMembers(request);
  AdvanceBallot();
}

void Leader::ElectionStateChanged(ElectionTracker* election) {
  if (election->GetState() == QuorumState::QUORUM_REACHED) {
    for (const auto& pair : election->accepted_slots) {
      auto slot = pair.first;
      auto tuple = pair.second;
      if (proposals_.count(slot) == 0 || tuple.ballot() > proposals_[slot].ballot) {
        proposals_[slot].ballot = tuple.ballot();
        proposals_[slot].value = tuple.value();
        proposals_[slot].is_chosen = true;
      }
    }
    is_elected_ = true;
    elected_leader_ = me_;
    LOG(INFO) << "I am the leader";
  }
}
*/

void Leader::StartNewAcceptance(uint32_t value) {
  proposals_[next_empty_slot_] = Proposal(ballot_, value);
  quorum_trackers_.emplace(
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
  if (acceptance->GetState() == QuorumState::QUORUM_REACHED) {
    auto slot = acceptance->slot;
    StartNewCommit(slot);
  }
  // TODO: Retransmit request after we implement heartbeat
}

void Leader::StartNewCommit(uint32_t slot) {
  quorum_trackers_.emplace(
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
    sender_->SendSameChannel(request, member);
  }
}


} // namespace slog