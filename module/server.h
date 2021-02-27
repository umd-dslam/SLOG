#pragma once

#include <glog/logging.h>

#include <chrono>
#include <set>
#include <thread>
#include <unordered_map>
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

struct PendingResponse {
  zmq::message_t identity;
  uint32_t stream_id;

  explicit PendingResponse(zmq::message_t&& identity, uint32_t stream_id)
      : identity(std::move(identity)), stream_id(stream_id) {}
};

class CompletedTransaction {
 public:
  explicit CompletedTransaction(const ConfigurationPtr& config, size_t involved_partitions)
      : required_copies_(config->replication_factor()), replicating_partitions_(involved_partitions) {
    partition_counters_.resize(config->num_partitions());
  }

  bool AddSubTxn(EnvelopePtr&& new_req) {
    DCHECK(new_req != nullptr);
    auto& subtxn = new_req->request().completed_subtxn();

    DCHECK(subtxn.partition() < partition_counters_.size());
    auto counter = ++partition_counters_[subtxn.partition()];
    if (counter == 1) {
      if (req_ == nullptr) {
        req_ = std::move(new_req);
      } else {
        auto txn = req_->mutable_request()->mutable_completed_subtxn()->mutable_txn();
        MergeTransaction(*txn, subtxn.txn());
      }
    }
    if (counter == required_copies_) {
      replicating_partitions_--;
    }

    return replicating_partitions_ == 0;
  }

  Transaction* ReleaseTxn() {
    if (req_ == nullptr) return nullptr;
    return req_->mutable_request()->mutable_completed_subtxn()->release_txn();
  }

 private:
  EnvelopePtr req_;
  uint32_t required_copies_;
  // Number of partitions that are still not fully replicated
  size_t replicating_partitions_;
  // Count number of subtxn copies received for each partition
  std::vector<uint32_t> partition_counters_;
};

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
  Server(const ConfigurationPtr& config, const std::shared_ptr<Broker>& broker,
         std::chrono::milliseconds poll_timeout = kModuleTimeout);

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
  void ProcessCompletedSubtxn(EnvelopePtr&& req);
  void ProcessStatsRequest(const internal::StatsRequest& stats_request);

  void SendTxnToClient(Transaction* txn);
  void SendResponseToClient(TxnId txn_id, api::Response&& res);

  TxnId NextTxnId();

  ConfigurationPtr config_;

  TxnId txn_id_counter_;
  std::unordered_map<TxnId, PendingResponse> pending_responses_;
  std::unordered_map<TxnId, CompletedTransaction> completed_txns_;

  std::mt19937 rg_;
};

}  // namespace slog