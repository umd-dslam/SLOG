#include "paxos/simple_multi_paxos.h"

#include "connection/broker.h"
#include "connection/sender.h"

namespace slog {

using internal::Request;
using internal::Response;

SimpleMultiPaxos::SimpleMultiPaxos(
    const string& group_name,
    const shared_ptr<Broker>& broker,
    const vector<string>& members,
    const string& me)
  : NetworkedModule(broker, group_name),
    leader_(*this, members, me),
    acceptor_(*this) {}

void SimpleMultiPaxos::HandleInternalRequest(
    Request&& req,
    string&& from_machine_id) {
  leader_.HandleRequest(req);
  acceptor_.HandleRequest(req, from_machine_id);
}

void SimpleMultiPaxos::HandleInternalResponse(
    Response&& res,
    string&& from_machine_id) {
  leader_.HandleResponse(res, from_machine_id);
}

bool SimpleMultiPaxos::IsMember() const {
  return leader_.IsMember();
}

void SimpleMultiPaxos::SendSameChannel(
    const google::protobuf::Message& msg,
    const std::string& to_machine_id) {
  Send(msg, GetName(), to_machine_id);
}

} // namespace slog