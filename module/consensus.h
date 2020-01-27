#pragma once

#include "paxos/simple_multi_paxos.h"

namespace slog {

class GlobalPaxos : public SimpleMultiPaxos {
public:
  GlobalPaxos(
      shared_ptr<Configuration> config,
      Broker& broker);

protected:
  void OnCommit(uint32_t slot, uint32_t value) final;

private:
  static vector<string> GetMembers(shared_ptr<Configuration> config);
};

class LocalPaxos : public SimpleMultiPaxos {
public:
  LocalPaxos(
      shared_ptr<Configuration> config,
      Broker& broker);

protected:
  void OnCommit(uint32_t slot, uint32_t value) final;

private:
  static vector<string> GetMembers(shared_ptr<Configuration> config);
};

} // namespace slog