#pragma once

#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/configuration.h"
#include "common/types.h"
#include "connection/broker.h"
#include "module/base/basic_module.h"
#include "module/scheduler_components/local_log.h"
#include "module/scheduler_components/batch_interleaver.h"
#include "module/scheduler_components/deterministic_lock_manager.h"
#include "module/scheduler_components/worker.h"
#include "storage/storage.h"

using std::shared_ptr;
using std::queue;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;

namespace slog {

struct TransactionHolder {
  unique_ptr<Transaction> txn;
  string worker;
  vector<internal::Request> early_remote_reads;
};

class Scheduler : public Module, ChannelHolder {
public:
  static const string WORKERS_ENDPOINT;

  Scheduler(
      shared_ptr<Configuration> config,
      zmq::context_t& context,
      Broker& broker,
      shared_ptr<Storage<Key, Record>> storage);

protected:
  void SetUp() final;

  void Loop() final;

private:
  friend class Worker;

  void HandleInternalRequest(
    internal::Request&& req,
    const string& from_machine_id);
  void HandleResponseFromWorker(internal::Response&& res);

  bool HasMessageFromChannel() const;
  bool HasMessageFromWorker() const;

  void ProcessForwardBatchRequest(
      internal::ForwardBatchRequest* forward_batch,
      const string& from_machine_id);

  void ProcessBatchOrder(
      const internal::PaxosOrder& order);

  void ProcessRemoteReadResult(
      internal::Request&& request);

  void TryUpdatingLocalLog();
  void TryProcessingNextBatchesFromGlobalLog();

  void DispatchTransaction(TxnId txn_id);

  void SendToWorker(internal::Request&& req, const string& worker);

  shared_ptr<Configuration> config_;
  zmq::socket_t worker_socket_;
  vector<zmq::pollitem_t> poll_items_;
  vector<unique_ptr<ModuleRunner>> workers_;
  queue<string> ready_workers_;

  unordered_map<uint32_t, LocalLog> all_local_logs_;
  BatchInterleaver interleaver_;
  DeterministicLockManager lock_manager_;
  unordered_map<TxnId, TransactionHolder> all_txns_;
};

} // namespace slog