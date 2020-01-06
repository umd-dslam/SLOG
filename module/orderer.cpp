#include "module/orderer.h"

namespace slog {

using internal::Request;

MultiHomeOrderer::MultiHomeOrderer(
    Broker& broker,
    const vector<string>& group_members,
    const string& me)
  : SimpleMultiPaxos("multi_home", broker, group_members, me) {}

void MultiHomeOrderer::OnCommit(uint32_t slot, uint32_t value) {
  Request req;
  auto order = req.mutable_order();
  order->set_slot(slot);
  order->set_value(value);
  SendSameMachine(req, SEQUENCER_CHANNEL);
}

LocalOrderer::LocalOrderer(
    Broker& broker,
    const vector<string>& group_members,
    const string& me)
  : SimpleMultiPaxos("lobal", broker, group_members, me) {}

void LocalOrderer::OnCommit(uint32_t slot, uint32_t value) {
  Request req;
  auto order = req.mutable_order();
  order->set_slot(slot);
  order->set_value(value);
  SendSameMachine(req, SCHEDULER_CHANNEL);
}

} // namespace slog