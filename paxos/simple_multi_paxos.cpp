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
    leader_(this, members, me),
    acceptor_(this) {}

void SimpleMultiPaxos::HandleInternalRequest(
    Request&& req,
    string&& from_machine_id,
    string&& /* from_channel */) {
  leader_.HandleRequest(req);
  acceptor_.HandleRequest(req, from_machine_id);
}

void SimpleMultiPaxos::HandleInternalResponse(
    Response&& res,
    string&& from_machine_id) {
  leader_.HandleResponse(res, from_machine_id);
}

const string SimpleMultiPaxosClient::CHANNEL_PREFIX = "paxos_client_";

SimpleMultiPaxosClient::SimpleMultiPaxosClient(Broker& broker, const string& group_name)
  : channel_(broker.AddChannel(CHANNEL_PREFIX + group_name)),
    group_name_(group_name) {}

void SimpleMultiPaxosClient::Propose(uint32_t value) {
  MMessage msg;
  Request req;
  auto paxos_propose = req.mutable_paxos_propose();
  paxos_propose->set_value(value);
  msg.Set(MM_PROTO, req);
  msg.Set(MM_FROM_CHANNEL, SimpleMultiPaxosClient::CHANNEL_PREFIX + group_name_);
  msg.Set(MM_TO_CHANNEL, SimpleMultiPaxos::CHANNEL_PREFIX + group_name_);
  channel_->Send(msg);
}

} // namespace slog