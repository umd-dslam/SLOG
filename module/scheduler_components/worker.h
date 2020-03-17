#pragma once

#include <unordered_set>

#include <zmq.hpp>

#include "common/configuration.h"
#include "common/types.h"
#include "module/base/module.h"
#include "module/scheduler_components/commands.h"
#include "proto/transaction.pb.h"
#include "proto/internal.pb.h"
#include "storage/storage.h"

using std::shared_ptr;
using std::unique_ptr;
using std::unordered_set;

namespace slog {

struct TransactionState {

  TransactionState(Transaction* txn) : txn(txn) {}

  Transaction* txn;
  unordered_set<uint32_t> awaited_passive_participants;
  unordered_set<uint32_t> active_participants;
  unordered_set<uint32_t> participants;
};

class Worker : public Module {
public:
  Worker(
      ConfigurationPtr config,
      zmq::context_t& context,
      shared_ptr<Storage<Key, Record>> storage);
  void SetUp() final;
  void Loop() final;

private:
  bool PrepareTransaction();
  bool ApplyRemoteReadResult(const internal::RemoteReadResult& read_result);
  void ExecuteAndCommitTransaction();

  void SendToScheduler(
      const google::protobuf::Message& req_or_res,
      string&& forward_to_machine = "");

  ConfigurationPtr config_;
  zmq::socket_t scheduler_socket_;
  shared_ptr<Storage<Key, Record>> storage_;
  unique_ptr<Commands> commands_;
  zmq::pollitem_t poll_item_;
  unique_ptr<TransactionState> txn_state_;
};

} // namespace slog