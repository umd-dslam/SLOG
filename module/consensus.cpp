#include "module/consensus.h"

#include "common/proto_utils.h"

namespace slog {

using internal::Request;

vector<string> GlobalPaxos::GetMembers(
    shared_ptr<Configuration> config) {
  auto part = config->GetLeaderPartitionForMultiHomeOrdering();
  vector<string> members;
  // Enlist a fixed machine at each region as members
  for (uint32_t rep = 0; rep < config->GetNumReplicas(); rep++) {
    members.push_back(MakeMachineId(rep, part));
  }
  return members;
}

GlobalPaxos::GlobalPaxos(
    shared_ptr<Configuration> config,
    Broker& broker)
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
  SendSameMachine(req, MULTI_HOME_ORDERER_CHANNEL);
}

vector<string> LocalPaxos::GetMembers(
    shared_ptr<Configuration> config) {
  auto local_rep = config->GetLocalReplica();
  vector<string> members;
  // Enlist all machines in the same region as members
  for (uint32_t part = 0; part < config->GetNumPartitions(); part++) {
    members.push_back(MakeMachineId(local_rep, part));
  }
  return members;
}

LocalPaxos::LocalPaxos(
    shared_ptr<Configuration> config,
    Broker& broker)
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
  SendSameMachine(req, SCHEDULER_CHANNEL);
}

} // namespace slog