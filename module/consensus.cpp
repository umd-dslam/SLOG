#include "module/consensus.h"

#include "common/proto_utils.h"

namespace slog {

using internal::Request;

vector<string> GlobalPaxos::GetMembers(const ConfigurationPtr& config) {
  auto part = config->GetLeaderPartitionForMultiHomeOrdering();
  vector<string> members;
  // Enlist a fixed machine at each region as members
  for (uint32_t rep = 0; rep < config->GetNumReplicas(); rep++) {
    auto machine_id = MakeMachineIdAsString(rep, part);
    members.push_back(machine_id);
  }
  return members;
}

GlobalPaxos::GlobalPaxos(
    const ConfigurationPtr& config,
    const shared_ptr<Broker>& broker)
  : SimpleMultiPaxos(
      GLOBAL_PAXOS,
      broker,
      GetMembers(config),
      config->GetLocalMachineIdAsString()) {}

void GlobalPaxos::OnCommit(uint32_t slot, uint32_t value) {
  Request req;
  auto order = req.mutable_forward_batch()->mutable_batch_order();
  order->set_slot(slot);
  order->set_batch_id(value);
  Send(req, MULTI_HOME_ORDERER_CHANNEL);
}

vector<string> LocalPaxos::GetMembers(const ConfigurationPtr& config) {
  auto local_rep = config->GetLocalReplica();
  vector<string> members;
  // Enlist all machines in the same region as members
  for (uint32_t part = 0; part < config->GetNumPartitions(); part++) {
    members.push_back(MakeMachineIdAsString(local_rep, part));
  }
  return members;
}

LocalPaxos::LocalPaxos(
    const ConfigurationPtr& config,
    const shared_ptr<Broker>& broker)
  : SimpleMultiPaxos(
      LOCAL_PAXOS,
      broker,
      GetMembers(config),
      config->GetLocalMachineIdAsString()) {}

void LocalPaxos::OnCommit(uint32_t slot, uint32_t value) {
  Request req;
  auto order = req.mutable_local_queue_order();
  order->set_queue_id(value);
  order->set_slot(slot);
  Send(req, INTERLEAVER_CHANNEL);
}

} // namespace slog