#include "paxos/simple_multi_paxos.h"

namespace slog {

using internal::Request;
using internal::Response;

const string SimpleMultiPaxos::CHANNEL_PREFIX = "paxos_";

SimpleMultiPaxos::SimpleMultiPaxos(
    const string& group_name,
    Broker& broker,
    const vector<string>& members,
    const string& me)
  : BasicModule(broker.AddChannel(CHANNEL_PREFIX + group_name)),
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

SimpleMultiPaxosClient::SimpleMultiPaxosClient(
    ChannelHolder& channel_holder, const string& group_name)
  : channel_holder_(channel_holder),
    group_name_(group_name) {}

void SimpleMultiPaxosClient::Propose(uint32_t value) {
  Request req;
  auto paxos_propose = req.mutable_paxos_propose();
  paxos_propose->set_value(value);
  channel_holder_.SendSameMachine(
      req, SimpleMultiPaxos::CHANNEL_PREFIX + group_name_);
}

} // namespace slog