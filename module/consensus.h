#pragma once

#include "paxos/simple_multi_paxos.h"

namespace slog {

class GlobalPaxos : public SimpleMultiPaxos {
public:
  GlobalPaxos(ConfigurationPtr config, Broker& broker);

protected:
  void OnCommit(uint32_t slot, uint32_t value) final;

private:
  static vector<string> GetMembers(ConfigurationPtr config);
};

class LocalPaxos : public SimpleMultiPaxos {
public:
  LocalPaxos(ConfigurationPtr config, Broker& broker);

protected:
  void OnCommit(uint32_t slot, uint32_t value) final;

private:
  static vector<string> GetMembers(ConfigurationPtr config);
};

} // namespace slog