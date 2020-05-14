#pragma once

#include <unordered_map>
#include <unordered_set>

#include <zmq.hpp>

#include "common/configuration.h"
#include "common/types.h"
#include "common/transaction_holder.h"
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
  TransactionState() = default;
  TransactionState(TransactionHolder* txn_holder) : txn_holder(txn_holder) {}

  TransactionHolder* txn_holder;
  bool has_local_reads;
  bool has_local_writes;
  // This set is reduced until we receive remote
  // reads from all passive partitions
  unordered_set<uint32_t> remote_passive_partitions;
  unordered_set<uint32_t> remote_active_partitions;
  uint32_t remote_reads_waiting_on;
};

class Worker : public Module {
public:
  Worker(
      const string& identity,
      ConfigurationPtr config,
      zmq::context_t& context,
      shared_ptr<Storage<Key, Record>> storage);
  void SetUp() final;
  void Loop() final;

private:
  void ProcessWorkerRequest(const internal::WorkerRequest& req);
  TransactionState& InitializeTransactionState(TransactionHolder* txn);
  void PopulateDataFromLocalStorage(Transaction* txn);

  void ProcessRemoteReadResult(const internal::RemoteReadResult& read_result);
  
  void ExecuteAndCommitTransaction(TxnId txn_id);
  void ExecuteTransactionHelper(TxnId txn_id);

  void SendToScheduler(
      const google::protobuf::Message& req_or_res,
      string&& forward_to_machine = "");

  std::string identity_;
  ConfigurationPtr config_;
  zmq::socket_t scheduler_socket_;
  shared_ptr<Storage<Key, Record>> storage_;
  unique_ptr<Commands> commands_;
  zmq::pollitem_t poll_item_;

  unordered_map<TxnId, TransactionState> txn_states_;
};

} // namespace slog