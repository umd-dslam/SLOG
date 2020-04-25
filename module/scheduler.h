#pragma once

#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/configuration.h"
#include "common/transaction_holder.h"
#include "common/types.h"
#include "connection/broker.h"
#include "data_structure/batch_log.h"
#include "module/base/basic_module.h"
#include "module/scheduler_components/batch_interleaver.h"
#include "module/scheduler_components/deterministic_lock_manager.h"
#include "module/scheduler_components/simple_remaster_manager.h"
#include "module/scheduler_components/worker.h"
#include "storage/storage.h"

using std::shared_ptr;
using std::queue;
using std::unique_ptr;
using std::map;
using std::unordered_map;
using std::unordered_set;
using std::vector;

namespace slog {

class Scheduler : public Module, ChannelHolder {
public:
  static const string WORKERS_ENDPOINT;
  static const uint32_t WORKER_LOAD_THRESHOLD;

  Scheduler(
      ConfigurationPtr config,
      zmq::context_t& context,
      Broker& broker,
      shared_ptr<Storage<Key, Record>> storage);

  void SetUp() final;

  void Loop() final;

private:
  friend class Worker;

  // A marker for the special log containing only multi-home txn
  const uint32_t kMultiHomeTxnLogMarker;

  void HandleInternalRequest(
    internal::Request&& req,
    const string& from_machine_id);
  void HandleResponseFromWorker(const internal::WorkerResponse& response);

  bool HasMessageFromChannel() const;
  bool HasMessageFromWorker() const;

  void ProcessForwardBatch(internal::ForwardBatch* forward_batch, const string& from_machine_id);
  void ProcessLocalQueueOrder(const internal::LocalQueueOrder& order);
  void ProcessRemoteReadResult(internal::Request&& request);
  void ProcessStatsRequest(const internal::StatsRequest& stats_request);

  void MaybeUpdateLocalLog();
  void MaybeProcessNextBatchesFromGlobalLog();

  bool AcceptTransaction(Transaction* txn);

  // Send single-home and lock-only transactions for counter checking
  void SendToRemasterManager(const TransactionHolder* txn_holder);
  // Send all transactions for locks, multi-home transactions are only registered
  void SendToLockManager(const TransactionHolder* txn_holder);
  void DispatchTransaction(TxnId txn_id);

  void SendToWorker(internal::Request&& req, const string& worker);

  // Abort and resubmit transaction, including all lock-onlies
  void AbortTransaction(const TransactionHolder* txn_holder);

  ConfigurationPtr config_;
  zmq::socket_t worker_socket_;
  vector<zmq::pollitem_t> poll_items_;
  vector<string> worker_identities_;
  vector<unique_ptr<ModuleRunner>> workers_;
  size_t next_worker_;

  unordered_map<uint32_t, BatchLog> all_logs_;
  BatchInterleaver local_interleaver_;
  DeterministicLockManager lock_manager_;
  SimpleRemasterManager remaster_manager_;

  unordered_map<TxnId, TransactionHolder> all_txns_;
  
  // Lock-only transactions are kept here during remaster checking
  map<TxnIdReplicaIdPair, TransactionHolder> lock_only_txns_;
};

} // namespace slog