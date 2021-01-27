#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/types.h"
#include "paxos/quorum_tracker.h"

using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;

namespace slog {

using EnvelopePtr = unique_ptr<internal::Envelope>;

class SimpleMultiPaxos;

struct Proposal {
  Proposal() : ballot(0), value(0), is_committed(false) {}

  Proposal(uint32_t ballot, uint32_t value) : ballot(ballot), value(value), is_committed(false) {}

  uint32_t ballot;
  uint32_t value;
  bool is_committed;
};

class Leader {
 public:
  /**
   * @param paxos   The enclosing Paxos class
   * @param members Machine Id of all members participating in this Paxos process
   * @param me      Machine Id of the current machine
   */
  Leader(SimpleMultiPaxos& paxos, const vector<MachineId>& members, MachineId me);

  void HandleRequest(const internal::Envelope& req);

  void HandleResponse(const internal::Envelope& res);

  bool IsMember() const;

 private:
  void ProcessCommitRequest(const internal::PaxosCommitRequest& commit);

  void StartNewAcceptance(uint32_t value);
  void AcceptanceStateChanged(const AcceptanceTracker* acceptance);

  void StartNewCommit(SlotId slot);
  void CommitStateChanged(const CommitTracker* commit);

  SimpleMultiPaxos& paxos_;

  const vector<MachineId> members_;
  const MachineId me_;
  bool is_elected_;
  bool is_member_;
  MachineId elected_leader_;

  SlotId min_uncommitted_slot_;
  SlotId next_empty_slot_;
  uint32_t ballot_;
  unordered_map<SlotId, Proposal> proposals_;
  vector<unique_ptr<QuorumTracker>> quorum_trackers_;
};
}  // namespace slog