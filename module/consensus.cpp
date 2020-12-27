#include "module/consensus.h"

#include "common/proto_utils.h"

namespace slog {

using internal::Request;

vector<MachineId> GlobalPaxos::GetMembers(const ConfigurationPtr& config) {
  auto part = config->leader_partition_for_multi_home_ordering();
  vector<MachineId> members;
  // Enlist a fixed machine at each region as members
  for (uint32_t rep = 0; rep < config->num_replicas(); rep++) {
    auto machine_id = config->MakeMachineId(rep, part);
    members.push_back(machine_id);
  }
  return members;
}

GlobalPaxos::GlobalPaxos(const ConfigurationPtr& config, const shared_ptr<Broker>& broker,
                         std::chrono::milliseconds poll_timeout)
    : SimpleMultiPaxos(kGlobalPaxos, broker, GetMembers(config), config->local_machine_id(), poll_timeout) {}

void GlobalPaxos::OnCommit(uint32_t slot, uint32_t value) {
  Request req;
  auto order = req.mutable_forward_batch()->mutable_batch_order();
  order->set_slot(slot);
  order->set_batch_id(value);
  Send(req, kMultiHomeOrdererChannel);
}

vector<MachineId> LocalPaxos::GetMembers(const ConfigurationPtr& config) {
  auto local_rep = config->local_replica();
  vector<MachineId> members;
  // Enlist all machines in the same region as members
  for (uint32_t part = 0; part < config->num_partitions(); part++) {
    members.push_back(config->MakeMachineId(local_rep, part));
  }
  return members;
}

LocalPaxos::LocalPaxos(const ConfigurationPtr& config, const shared_ptr<Broker>& broker,
                       std::chrono::milliseconds poll_timeout)
    : SimpleMultiPaxos(kLocalPaxos, broker, GetMembers(config), config->local_machine_id(), poll_timeout) {}

void LocalPaxos::OnCommit(uint32_t slot, uint32_t value) {
  Request req;
  auto order = req.mutable_local_queue_order();
  order->set_queue_id(value);
  order->set_slot(slot);
  Send(req, kInterleaverChannel);
}

}  // namespace slog