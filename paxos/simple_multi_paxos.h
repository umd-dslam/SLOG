#pragma once

#include "connection/broker.h"
#include "module/base/networked_module.h"
#include "paxos/acceptor.h"
#include "paxos/leader.h"

using std::shared_ptr;
using std::string;

namespace slog {

class SimpleMultiPaxos : public NetworkedModule {
public:

  /**
   * @param group_name  Name of the current paxos group. Used to differentiate messages 
   *                    from other paxos groups
   * @param broker      The broker for sending and receiving messages
   * @param members     Machine Id of all members participating in this Paxos process
   * @param me          Machine Id of the current machine
   */
  SimpleMultiPaxos(
      const string& group_name,
      const shared_ptr<Broker>& broker,
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

  void SendSameChannel(
      const google::protobuf::Message& request_or_response,
      const std::string& to_machine_id);

  friend class Leader;
  friend class Acceptor;
};

} // namespace slog