#include "paxos/simple_multi_paxos.h"

#include "connection/broker.h"
#include "connection/sender.h"

namespace slog {

using internal::Request;
using internal::Response;

SimpleMultiPaxos::SimpleMultiPaxos(Channel group_number, const shared_ptr<Broker>& broker,
                                   const vector<MachineId>& members, MachineId me,
                                   std::chrono::milliseconds poll_timeout)
    : NetworkedModule("Paxos-" + std::to_string(group_number), broker, group_number, poll_timeout),
      leader_(*this, members, me),
      acceptor_(*this) {}

void SimpleMultiPaxos::HandleInternalRequest(EnvelopePtr&& req) {
  // A non-leader machine can still need to do some work to maintain its state should it becomes a leader later
  leader_.HandleRequest(*req);
  acceptor_.HandleRequest(*req);
}

void SimpleMultiPaxos::HandleInternalResponse(EnvelopePtr&& res) { leader_.HandleResponse(*res); }

bool SimpleMultiPaxos::IsMember() const { return leader_.IsMember(); }

void SimpleMultiPaxos::SendSameChannel(const internal::Envelope& env, MachineId to_machine_id) {
  Send(env, to_machine_id, channel());
}

void SimpleMultiPaxos::SendSameChannel(EnvelopePtr&& env, MachineId to_machine_id) {
  Send(std::move(env), to_machine_id, channel());
}

void SimpleMultiPaxos::SendSameChannel(EnvelopePtr&& env, const std::vector<MachineId>& to_machine_ids) {
  Send(std::move(env), to_machine_ids, channel());
}


}  // namespace slog