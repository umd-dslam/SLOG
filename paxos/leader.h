#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "paxos/quorum_tracker.h"

using std::string;
using std::unordered_map;
using std::unordered_set;
using std::vector;

namespace slog {

class SimpleMultiPaxos;

struct Proposal {
  Proposal()
    : ballot(0), value(0), is_committed(false) {}

  Proposal(uint32_t ballot, uint32_t value)
    : ballot(ballot), value(value), is_committed(false) {}

  uint32_t ballot;
  uint32_t value;
  bool is_committed;
};

class Leader {
public:
  /**
   * @param sender  The enclosing Paxos class
   * @param members Machine Id of all members participating in this Paxos process
   * @param me      Machine Id of the current machine
   */
  Leader(
      SimpleMultiPaxos& sender,
      const vector<string>& members,
      const string& me);

  void HandleRequest(const internal::Request& req);

  void HandleResponse(
      const internal::Response& res,
      const string& from_machine_id);

private:
  void HandleCommitRequest(const internal::PaxosCommitRequest commit);

  void StartNewAcceptance(uint32_t value);
  void AcceptanceStateChanged(AcceptanceTracker* acceptance);

  void StartNewCommit(uint32_t slot);
  void CommitStateChanged(CommitTracker* commit);

  void SendToAllMembers(const internal::Request& request);

  SimpleMultiPaxos& sender_;

  const vector<string> members_;
  const string me_;
  bool is_elected_;
  string elected_leader_;

  uint32_t min_uncommitted_slot_;
  uint32_t next_empty_slot_;
  uint32_t ballot_;
  unordered_map<uint32_t, Proposal> proposals_;
  vector<unique_ptr<QuorumTracker>> quorum_trackers_;
};
} // namespace slog