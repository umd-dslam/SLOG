#pragma once

#include "connection/broker.h"
#include "module/base/networked_module.h"
#include "paxos/acceptor.h"
#include "paxos/leader.h"

using std::shared_ptr;
using std::string;

namespace slog {

class SimulatedMultiPaxos : public NetworkedModule {
 public:
  /**
   * @param group_number  Number of the current paxos group. Used to differentiate messages
   *                      from other paxos groups
   * @param broker        The broker for sending and receiving messages
   * @param members       Machine Id of all members participating in this Paxos process
   * @param me            Machine Id of the current machine
   */
  SimulatedMultiPaxos(Channel group_number, const shared_ptr<Broker>& broker, const vector<MachineId>& group_members,
                      MachineId me, std::chrono::milliseconds poll_timeout = kModuleTimeout);

  std::string name() const override { return "Paxos-" + std::to_string(channel()); }

  bool IsMember() const;

 protected:
  void OnInternalRequestReceived(EnvelopePtr&& env) final;
  void OnInternalResponseReceived(EnvelopePtr&& env) final;

  virtual void OnCommit(uint32_t slot, uint32_t value, MachineId leader) = 0;

 private:
  Leader leader_;
  Acceptor acceptor_;

  void SendSameChannel(const internal::Envelope& env, MachineId to_machine_id);
  void SendSameChannel(EnvelopePtr&& env, MachineId to_machine_id);
  void SendSameChannel(EnvelopePtr&& env, const std::vector<MachineId>& to_machine_ids);

  friend class Leader;
  friend class Acceptor;
};

}  // namespace slog