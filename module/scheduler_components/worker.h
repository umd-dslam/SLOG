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
  enum class Phase {
    READ_LOCAL_STORAGE,
    WAIT_REMOTE_READ,
    EXECUTE,
    WAIT_CONFIRMATION,
    COMMIT,
    FINISH,
  };

  TransactionState() = default;
  TransactionState(TransactionHolder* txn_holder) : txn_holder(txn_holder) {}
  TransactionHolder* txn_holder = nullptr;
  uint32_t remote_messages_waiting_on = 0;
  Phase phase = Phase::READ_LOCAL_STORAGE;
};

/**
 * 
 */
class Worker : public Module {
public:
  Worker(
      const string& identity,
      const ConfigurationPtr& config,
      zmq::context_t& context,
      const shared_ptr<Storage<Key, Record>>& storage);
  void SetUp() final;
  void Loop() final;

private:
  TxnId ProcessWorkerRequest(const internal::WorkerRequest& req);
  TxnId ProcessRemoteReadResult(const internal::RemoteReadResult& read_result);
  void AdvanceTransaction(TxnId txn_id);

  void ReadLocalStorage(TxnId txn_id);
  void Execute(TxnId txn_id);
  void Commit(TxnId txn_id);
  void Finish(TxnId txn_id);

  void SendToOtherPartitions(
      internal::Request&& request,
      const std::unordered_set<uint32_t>& partitions);
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