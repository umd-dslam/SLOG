#pragma once

#include "common/types.h"
#include "proto/internal.pb.h"

using std::string;

namespace slog {

class SimulatedMultiPaxos;

class Acceptor {
 public:
  /**
   * @param sender The enclosing Paxos class
   */
  Acceptor(SimulatedMultiPaxos& sender);

  void HandleRequest(const internal::Envelope& req);

 private:
  void ProcessAcceptRequest(const internal::PaxosAcceptRequest& req, MachineId from_machine_id);

  void ProcessCommitRequest(const internal::PaxosCommitRequest& req, MachineId from_machine_id);

  SimulatedMultiPaxos& sender_;

  uint32_t ballot_;
};

}  // namespace slog