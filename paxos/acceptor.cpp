#include "paxos/acceptor.h"

#include "paxos/simple_multi_paxos.h"

namespace slog {

using internal::Request;
using internal::Response;

Acceptor::Acceptor(SimpleMultiPaxos& sender) : sender_(sender), ballot_(0) {}

void Acceptor::HandleRequest(
    const Request& req,
    MachineIdNum from_machine_id) {
  switch (req.type_case()) {
    case Request::TypeCase::kPaxosAccept:
      ProcessAcceptRequest(req.paxos_accept(), from_machine_id); break;
    case Request::TypeCase::kPaxosCommit:
      ProcessCommitRequest(req.paxos_commit(), from_machine_id); break;
    default:
      break;
  }
}

void Acceptor::ProcessAcceptRequest(
    const internal::PaxosAcceptRequest& req,
    MachineIdNum from_machine_id) {
  if (req.ballot() < ballot_) {
    return;
  }
  ballot_ = req.ballot();
  Response res;
  auto accept_response = res.mutable_paxos_accept();
  accept_response->set_ballot(ballot_);
  accept_response->set_slot(req.slot());
  sender_.SendSameChannel(res, from_machine_id);
}

void Acceptor::ProcessCommitRequest(
    const internal::PaxosCommitRequest& req,
    MachineIdNum from_machine_id) {
  // TODO: If leader election is implemented, this is where we erase
  //       memory about an accepted value
  Response res;
  auto commit_response = res.mutable_paxos_commit();
  commit_response->set_slot(req.slot());
  sender_.SendSameChannel(res, from_machine_id);
}

} // namespace slog