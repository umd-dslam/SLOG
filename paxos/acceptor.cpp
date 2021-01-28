#include "paxos/acceptor.h"

#include "paxos/simulated_multi_paxos.h"

namespace slog {

using internal::Envelope;
using internal::Request;
using internal::Response;

Acceptor::Acceptor(SimulatedMultiPaxos& sender) : sender_(sender), ballot_(0) {}

void Acceptor::HandleRequest(const internal::Envelope& req) {
  switch (req.request().type_case()) {
    case Request::TypeCase::kPaxosAccept:
      ProcessAcceptRequest(req.request().paxos_accept(), req.from());
      break;
    case Request::TypeCase::kPaxosCommit:
      ProcessCommitRequest(req.request().paxos_commit(), req.from());
      break;
    default:
      break;
  }
}

void Acceptor::ProcessAcceptRequest(const internal::PaxosAcceptRequest& req, MachineId from_machine_id) {
  if (req.ballot() < ballot_) {
    return;
  }
  ballot_ = req.ballot();
  auto env = sender_.NewEnvelope();
  auto accept_response = env->mutable_response()->mutable_paxos_accept();
  accept_response->set_ballot(ballot_);
  accept_response->set_slot(req.slot());
  sender_.SendSameChannel(move(env), from_machine_id);
}

void Acceptor::ProcessCommitRequest(const internal::PaxosCommitRequest& req, MachineId from_machine_id) {
  // TODO: If leader election is implemented, this is where we erase
  //       memory about an accepted value
  auto env = sender_.NewEnvelope();
  auto commit_response = env->mutable_response()->mutable_paxos_commit();
  commit_response->set_slot(req.slot());
  sender_.SendSameChannel(move(env), from_machine_id);
}

}  // namespace slog