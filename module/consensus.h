#pragma once

#include "paxos/simulated_multi_paxos.h"

namespace slog {

class GlobalPaxos : public SimulatedMultiPaxos {
 public:
  GlobalPaxos(const ConfigurationPtr& config, const std::shared_ptr<Broker>& broker,
              std::chrono::milliseconds poll_timeout = kModuleTimeout);

 protected:
  void OnCommit(uint32_t slot, uint32_t value, bool is_leader) final;

 private:
  vector<MachineId> multihome_orderers_;
};

class LocalPaxos : public SimulatedMultiPaxos {
 public:
  LocalPaxos(const ConfigurationPtr& config, const std::shared_ptr<Broker>& broker,
             std::chrono::milliseconds poll_timeout = kModuleTimeout);

 protected:
  void OnCommit(uint32_t slot, uint32_t value, bool) final;
};

}  // namespace slog