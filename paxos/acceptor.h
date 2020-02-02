#pragma once

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

  void HandleRequest(
      const internal::Request& req,
      const string& from_machine_id);

private:
  void ProcessAcceptRequest(
      const internal::PaxosAcceptRequest& req,
      const string& from_machine_id);
  
  void ProcessCommitRequest(
      const internal::PaxosCommitRequest& req,
      const string& from_machine_id);

  SimpleMultiPaxos& sender_;

  uint32_t ballot_;
};

} // namespace slog