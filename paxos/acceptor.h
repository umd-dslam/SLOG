#pragma once

#include "proto/internal.pb.h"

using std::string;

namespace slog {

class SimpleMultiPaxos;

class Acceptor {
public:
  Acceptor(SimpleMultiPaxos* sender);

  void HandleRequest(
      const internal::Request& req,
      const string& from_machine_id);

private:
  void HandleAcceptRequest(
      const internal::PaxosAcceptRequest& req,
      const string& from_machine_id);
  
  void HandleCommitRequest(
      const internal::PaxosCommitRequest& req,
      const string& from_machine_id);

  SimpleMultiPaxos* const sender_;

  uint32_t ballot_;
};

} // namespace slog