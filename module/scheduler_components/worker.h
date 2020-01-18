#pragma once

#include <unordered_set>

#include <zmq.hpp>

#include "benchmark/stored_procedures.h"
#include "common/types.h"
#include "module/base/module.h"
#include "proto/transaction.pb.h"
#include "proto/internal.pb.h"
#include "storage/storage.h"

using std::shared_ptr;
using std::unique_ptr;
using std::unordered_set;

namespace slog {

class Scheduler;

struct TransactionState {

  TransactionState(TxnId txn_id) : txn_id(txn_id) {}

  TxnId txn_id;
  unordered_set<uint32_t> awaited_passive_participants;
  unordered_set<uint32_t> active_participants;
};

class Worker : public Module {
public:
  Worker(
      Scheduler& scheduler,
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

  Scheduler& scheduler_;
  zmq::socket_t scheduler_socket_;
  shared_ptr<Storage<Key, Record>> storage_;
  unique_ptr<StoredProcedures> stored_procedures_;
  zmq::pollitem_t poll_item_;
  unique_ptr<TransactionState> txn_state_;
};

} // namespace slog