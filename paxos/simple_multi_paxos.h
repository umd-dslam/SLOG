#pragma once

#include "connection/broker.h"
#include "module/base/basic_module.h"
#include "paxos/acceptor.h"
#include "paxos/paxos_client.h"
#include "paxos/leader.h"

using std::shared_ptr;
using std::string;

namespace slog {

class SimpleMultiPaxos : public BasicModule {
public:
  static const string CHANNEL_PREFIX;

  /**
   * @param group_name  Name of the current paxos group. Used to differentiate messages 
   *                    from other paxos groups
   * @param broker      The broker for sending and receiving messages
   * @param members     Machine Id of all members participating in this Paxos process
   * @param me          Machine Id of the current machine
   */
  SimpleMultiPaxos(
      const string& group_name,
      Broker& broker,
      const vector<string>& group_members,
      const string& me);

  bool IsMember() const;

protected:
  void HandleInternalRequest(
      internal::Request&& req,
      string&& from_machine_id) final;

  void HandleInternalResponse(
      internal::Response&& res,
      string&& from_machine_id) final;

  virtual void OnCommit(uint32_t slot, uint32_t value) = 0;

private:
  Leader leader_;
  Acceptor acceptor_;

  friend class Leader;
  friend class Acceptor;
};

class SimpleMultiPaxosClient : public PaxosClient {
public:
  SimpleMultiPaxosClient(ChannelHolder& channel_holder, const string& group_name);

  void Propose(uint32_t value) final;

private:

  ChannelHolder& channel_holder_;
  const string group_name_;
};

} // namespace slog