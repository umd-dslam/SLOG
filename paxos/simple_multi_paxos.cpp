#include "paxos/simple_multi_paxos.h"

#include "connection/broker.h"
#include "connection/sender.h"

namespace slog {

using internal::Request;
using internal::Response;

SimpleMultiPaxos::SimpleMultiPaxos(
    Channel group_channel,
    const shared_ptr<Broker>& broker,
    const vector<MachineIdNum>& members,
    MachineIdNum me)
  : NetworkedModule(broker, group_channel),
    leader_(*this, members, me),
    acceptor_(*this) {}

void SimpleMultiPaxos::HandleInternalRequest(Request&& req, MachineIdNum from) {
  leader_.HandleRequest(req);
  acceptor_.HandleRequest(req, from);
}

void SimpleMultiPaxos::HandleInternalResponse(Response&& res, MachineIdNum from) {
  leader_.HandleResponse(res, from);
}

bool SimpleMultiPaxos::IsMember() const {
  return leader_.IsMember();
}

void SimpleMultiPaxos::SendSameChannel(
    const google::protobuf::Message& msg,
    MachineIdNum to_machine_id) {
  Send(msg, channel(), to_machine_id);
}

} // namespace slog