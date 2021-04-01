#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/types.h"
#include "proto/internal.pb.h"

using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;

namespace slog {

using EnvelopePtr = unique_ptr<internal::Envelope>;

class SimulatedMultiPaxos;

struct PaxosInstance {
  PaxosInstance(uint32_t ballot, uint64_t value) : ballot(ballot), value(value), num_accepts(0), num_commits(0) {}

  uint32_t ballot;
  uint64_t value;
  int num_accepts;
  int num_commits;
};

class Leader {
 public:
  /**
   * @param paxos   The enclosing Paxos class
   * @param members Machine Id of all members participating in this Paxos process
   * @param me      Machine Id of the current machine
   */
  Leader(SimulatedMultiPaxos& paxos, const vector<MachineId>& members, MachineId me);

  void HandleRequest(const internal::Envelope& req);
  void HandleResponse(const internal::Envelope& res);

  bool IsMember() const;

 private:
  void ProcessCommitRequest(const internal::PaxosCommitRequest& commit);
  void StartNewInstance(uint64_t value);

  SimulatedMultiPaxos& paxos_;

  const vector<MachineId> members_;
  vector<MachineId> acceptors_;
  const MachineId me_;
  bool is_elected_;
  bool is_member_;
  MachineId elected_leader_;

  SlotId next_empty_slot_;
  uint32_t ballot_;
  unordered_map<SlotId, PaxosInstance> instances_;
};
}  // namespace slog