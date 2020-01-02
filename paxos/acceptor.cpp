#include "paxos/acceptor.h"

#include "paxos/simple_multi_paxos.h"

namespace slog {

using internal::Request;
using internal::Response;

Acceptor::Acceptor(SimpleMultiPaxos* sender) : sender_(sender) {}

void Acceptor::HandleRequest(
    const Request& req,
    const string& from_machine_id) {
  switch (req.type_case()) {
    case Request::TypeCase::kPaxosElect:
      HandleElectRequest(req.paxos_elect(), from_machine_id); break;
    case Request::TypeCase::kPaxosAccept:
      HandleAcceptRequest(req.paxos_accept(), from_machine_id); break;
    case Request::TypeCase::kPaxosCommit:
      HandleCommitRequest(req.paxos_commit(), from_machine_id); break;
    default:
      break;
  }
}

void HandleElectRequest(
    const internal::PaxosElectRequest& /* req */,
    const string& /* from_machine_id */) {

}

void Acceptor::HandleAcceptRequest(
    const internal::PaxosAcceptRequest& req,
    const string& from_machine_id) {
  if (req.ballot() < ballot_) {
    return;
  }
  ballot_ = req.ballot();
  Response res;
  auto accept_response = res.mutable_paxos_accept();
  accept_response->set_ballot(ballot_);
  accept_response->set_slot(req.slot());
  sender_->SendSameChannel(res, from_machine_id);
}

void Acceptor::HandleCommitRequest(
    const internal::PaxosCommitRequest& req,
    const string& from_machine_id) {
  Response res;
  auto commit_response = res.mutable_paxos_commit();
  commit_response->set_slot(req.slot());
  sender_->SendSameChannel(res, from_machine_id);
}

} // namespace slog