#pragma once

#include "common/types.h"
#include "proto/internal.pb.h"

using std::string;

namespace slog {

class SimpleMultiPaxos;

class Acceptor {
 public:
  /**
   * @param sender The enclosing Paxos class
   */
  Acceptor(SimpleMultiPaxos& sender);

  void HandleRequest(const internal::Request& req, MachineId from);

 private:
  void ProcessAcceptRequest(const internal::PaxosAcceptRequest& req, MachineId from_machine_id);

  void ProcessCommitRequest(const internal::PaxosCommitRequest& req, MachineId from_machine_id);

  SimpleMultiPaxos& sender_;

  uint32_t ballot_;
};

}  // namespace slog