#pragma once

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
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;

namespace slog {

using TransactionPtr = unique_ptr<Transaction>;

class Scheduler : public Module, ChannelHolder {
public:
  static const string WORKER_IN;

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

  void ProcessOrderRequest(
      const internal::OrderRequest& order);

  void TryProcessingNextBatchesFromGlobalLog();

  void DispatchTransaction(TxnId txn_id);

  shared_ptr<Configuration> config_;
  zmq::socket_t worker_socket_;
  vector<unique_ptr<ModuleRunner>> workers_;
  vector<zmq::pollitem_t> poll_items_;

  unordered_map<uint32_t, LocalLog> local_logs_;
  BatchInterleaver interleaver_;
  DeterministicLockManager lock_manager_;
  unordered_map<TxnId, TransactionPtr> all_txns_;
};

} // namespace slog