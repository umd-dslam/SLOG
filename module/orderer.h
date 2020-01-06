#pragma once

#include "paxos/simple_multi_paxos.h"

namespace slog {

class MultiHomeOrderer : public SimpleMultiPaxos {
public:
  MultiHomeOrderer(
      Broker& broker,
      const vector<string>& group_members,
      const string& me);

protected:
  void OnCommit(uint32_t slot, uint32_t value) final;
};

class LocalOrderer : public SimpleMultiPaxos {
public:
  LocalOrderer(
      Broker& broker,
      const vector<string>& group_members,
      const string& me);

protected:
  void OnCommit(uint32_t slot, uint32_t value) final;
};

} // namespace slog