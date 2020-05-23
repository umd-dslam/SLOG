#pragma once

#include "paxos/simple_multi_paxos.h"

namespace slog {

class GlobalPaxos : public SimpleMultiPaxos {
public:
  GlobalPaxos(const ConfigurationPtr& config, Broker& broker);

protected:
  void OnCommit(uint32_t slot, uint32_t value) final;

private:
  static vector<string> GetMembers(const ConfigurationPtr& config);
};

class LocalPaxos : public SimpleMultiPaxos {
public:
  LocalPaxos(const ConfigurationPtr& config, Broker& broker);

protected:
  void OnCommit(uint32_t slot, uint32_t value) final;

private:
  static vector<string> GetMembers(const ConfigurationPtr& config);
};

} // namespace slog