#pragma once

#include "paxos/simple_multi_paxos.h"

namespace slog {

class GlobalPaxos : public SimpleMultiPaxos {
public:
  GlobalPaxos(
      const ConfigurationPtr& config,
      const std::shared_ptr<Broker>& broker,
      int poll_timeout_ms = kModuleTimeoutMs);

protected:
  void OnCommit(uint32_t slot, uint32_t value) final;

private:
  static vector<MachineId> GetMembers(const ConfigurationPtr& config);
};

class LocalPaxos : public SimpleMultiPaxos {
public:
  LocalPaxos(
      const ConfigurationPtr& config,
      const std::shared_ptr<Broker>& broker,
      int poll_timeout_ms = kModuleTimeoutMs);

protected:
  void OnCommit(uint32_t slot, uint32_t value) final;

private:
  static vector<MachineId> GetMembers(const ConfigurationPtr& config);
};

} // namespace slog