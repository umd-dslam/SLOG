#pragma once

#include "paxos/simulated_multi_paxos.h"

namespace slog {

class GlobalPaxos : public SimulatedMultiPaxos {
 public:
  GlobalPaxos(const std::shared_ptr<Broker>& broker, std::chrono::milliseconds poll_timeout = kModuleTimeout);

 protected:
  void OnCommit(uint32_t slot, uint32_t value, MachineId leader) final;

 private:
  MachineId local_machine_id_;
  vector<MachineId> multihome_orderers_;
};

class LocalPaxos : public SimulatedMultiPaxos {
 public:
  LocalPaxos(const std::shared_ptr<Broker>& broker, std::chrono::milliseconds poll_timeout = kModuleTimeout);

 protected:
  void OnCommit(uint32_t slot, uint32_t value, MachineId leader) final;
};

}  // namespace slog