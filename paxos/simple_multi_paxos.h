#pragma once

#include "connection/broker.h"
#include "module/basic_module.h"
#include "paxos/acceptor.h"
#include "paxos/paxos_client.h"
#include "paxos/leader.h"

using std::shared_ptr;
using std::string;

namespace slog {

class SimpleMultiPaxos : public BasicModule {
public:
  static const string CHANNEL_PREFIX;

  SimpleMultiPaxos(
      Broker& broker,
      const string& group_name,
      const vector<string>& group_members,
      const string& me);

protected:
  void HandleInternalRequest(
      internal::Request&& req,
      string&& from_machine_id,
      string&& from_channel) final;

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

class SimpleMultiPaxosClient : PaxosClient {
public:
  static const string CHANNEL_PREFIX;

  SimpleMultiPaxosClient(Broker& broker, const string& group_name);

  void Append(uint32_t number) final;

private:

  unique_ptr<Channel> channel_;
  const string group_name_;
};

} // namespace slog