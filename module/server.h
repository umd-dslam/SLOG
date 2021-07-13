#pragma once

#include <glog/logging.h>

#include <chrono>
#include <set>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <zmq.hpp>

#include "common/configuration.h"
#include "common/proto_utils.h"
#include "common/types.h"
#include "connection/broker.h"
#include "module/base/networked_module.h"
#include "proto/api.pb.h"
#include "storage/lookup_master_index.h"

namespace slog {

/**
 * A Server serves external requests from the clients.
 *
 * INPUT:  External TransactionRequest
 *
 * OUTPUT: For external TransactionRequest, it forwards the txn internally
 *         to appropriate modules and waits for internal responses before
 *         responding back to the client with an external TransactionResponse.
 */
class Server : public NetworkedModule {
 public:
  Server(const std::shared_ptr<Broker>& broker, const MetricsRepositoryManagerPtr& metrics_manager,
         std::chrono::milliseconds poll_timeout = kModuleTimeout);

  std::string name() const override { return "Server"; }

 protected:
  void Initialize() final;

  /**
   * After a transaction is processed by different partitions, each
   * involving partition will send a sub-transaction with the processing
   * result to the coordinating server. The coordinating server will be
   * in charge of merging these sub-transactions and responding back to
   * the client.
   */
  void OnInternalRequestReceived(EnvelopePtr&& env) final;

  void OnInternalResponseReceived(EnvelopePtr&& env) final;

  bool OnCustomSocket() final;

 private:
  void ProcessFinishedSubtxn(EnvelopePtr&& req);
  void ProcessStatsRequest(const internal::StatsRequest& stats_request);

  void SendTxnToClient(Transaction* txn);
  void SendResponseToClient(TxnId txn_id, api::Response&& res);

  TxnId NextTxnId();

  TxnId txn_id_counter_;

  struct PendingResponse {
    zmq::message_t identity;
    uint32_t stream_id;

    explicit PendingResponse(zmq::message_t&& identity, uint32_t stream_id)
        : identity(std::move(identity)), stream_id(stream_id) {}
  };
  std::unordered_map<TxnId, PendingResponse> pending_responses_;

  class FinishedTransaction {
   public:
    FinishedTransaction(size_t involved_partitions);
    bool AddSubTxn(EnvelopePtr&& new_req);
    Transaction* ReleaseTxn();

   private:
    EnvelopePtr req_;
    size_t remaining_partitions_;
  };
  std::unordered_map<TxnId, FinishedTransaction> finished_txns_;

  std::unordered_set<MachineId> offline_machines_;
};

}  // namespace slog