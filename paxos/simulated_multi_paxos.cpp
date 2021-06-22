#include "paxos/simulated_multi_paxos.h"

#include "connection/broker.h"
#include "connection/sender.h"

namespace slog {

using internal::Request;
using internal::Response;

SimulatedMultiPaxos::SimulatedMultiPaxos(Channel group_number, const shared_ptr<Broker>& broker,
                                         const vector<MachineId>& members, MachineId me,
                                         std::chrono::milliseconds poll_timeout)
    : NetworkedModule(broker, group_number, nullptr, poll_timeout), leader_(*this, members, me), acceptor_(*this) {}

void SimulatedMultiPaxos::OnInternalRequestReceived(EnvelopePtr&& req) {
  // A non-leader machine can still need to do some work to maintain its state should it becomes a leader later
  leader_.HandleRequest(*req);
  acceptor_.HandleRequest(*req);
}

void SimulatedMultiPaxos::OnInternalResponseReceived(EnvelopePtr&& res) { leader_.HandleResponse(*res); }

bool SimulatedMultiPaxos::IsMember() const { return leader_.IsMember(); }

void SimulatedMultiPaxos::SendSameChannel(const internal::Envelope& env, MachineId to_machine_id) {
  Send(env, to_machine_id, channel());
}

void SimulatedMultiPaxos::SendSameChannel(EnvelopePtr&& env, MachineId to_machine_id) {
  Send(std::move(env), to_machine_id, channel());
}

void SimulatedMultiPaxos::SendSameChannel(EnvelopePtr&& env, const std::vector<MachineId>& to_machine_ids) {
  Send(std::move(env), to_machine_ids, channel());
}

}  // namespace slog