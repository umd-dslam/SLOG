#include "module/consensus.h"

#include "common/proto_utils.h"

namespace slog {

using internal::Request;

vector<MachineIdNum> GlobalPaxos::GetMembers(const ConfigurationPtr& config) {
  auto part = config->GetLeaderPartitionForMultiHomeOrdering();
  vector<MachineIdNum> members;
  // Enlist a fixed machine at each region as members
  for (uint32_t rep = 0; rep < config->GetNumReplicas(); rep++) {
    auto machine_id = config->MakeMachineIdNum(rep, part);
    members.push_back(machine_id);
  }
  return members;
}

GlobalPaxos::GlobalPaxos(
    const ConfigurationPtr& config,
    const shared_ptr<Broker>& broker)
  : SimpleMultiPaxos(
      kGlobalPaxos,
      broker,
      GetMembers(config),
      config->GetLocalMachineIdAsNumber()) {}

void GlobalPaxos::OnCommit(uint32_t slot, uint32_t value) {
  Request req;
  auto order = req.mutable_forward_batch()->mutable_batch_order();
  order->set_slot(slot);
  order->set_batch_id(value);
  Send(req, kMultiHomeOrdererChannel);
}

vector<MachineIdNum> LocalPaxos::GetMembers(const ConfigurationPtr& config) {
  auto local_rep = config->GetLocalReplica();
  vector<MachineIdNum> members;
  // Enlist all machines in the same region as members
  for (uint32_t part = 0; part < config->GetNumPartitions(); part++) {
    members.push_back(config->MakeMachineIdNum(local_rep, part));
  }
  return members;
}

LocalPaxos::LocalPaxos(
    const ConfigurationPtr& config,
    const shared_ptr<Broker>& broker)
  : SimpleMultiPaxos(
      kLocalPaxos,
      broker,
      GetMembers(config),
      config->GetLocalMachineIdAsNumber()) {}

void LocalPaxos::OnCommit(uint32_t slot, uint32_t value) {
  Request req;
  auto order = req.mutable_local_queue_order();
  order->set_queue_id(value);
  order->set_slot(slot);
  Send(req, kInterleaverChannel);
}

} // namespace slog