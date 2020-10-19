#pragma once

#include <optional>
#include <vector>

#include "common/configuration.h"
#include "common/message_pool.h"
#include "common/types.h"
#include "proto/transaction.pb.h"

namespace slog {

using TxnIdReplicaIdPair = std::pair<uint32_t, uint32_t>;
using ReusableRequest = ReusableMessage<internal::Request>;

class TransactionHolder {
public:
  TransactionHolder();
  TransactionHolder(const ConfigurationPtr& config, ReusableRequest&& req);

  void SetTxnRequest(const ConfigurationPtr& config, ReusableRequest&& req);
  ReusableRequest& request() { return txn_request_; }
  Transaction* transaction() const {
    if (txn_request_.get() == nullptr) {
      return nullptr;
    }
    return txn_request_.get()->mutable_forward_txn()->mutable_txn();
  }

  void SetWorker(uint32_t worker) {
    worker_ = worker;
  }

  std::optional<uint32_t> worker() const {
    return worker_;
  }

  const std::vector<std::pair<Key, LockMode>>& keys_in_partition() const;
  const std::unordered_set<uint32_t>& involved_partitions() const;
  const std::unordered_set<uint32_t>& active_partitions() const;
  const std::unordered_set<uint32_t>& involved_replicas() const;

  std::vector<ReusableRequest>& early_remote_reads();

  /**
   * Get the id of the replica where this transaction was added to the local log.
   * Should only be used for single-home and lock-only transactions.
   */
  uint32_t replica_id() const;
  static uint32_t replica_id(const Transaction* txn);

  /**
   * Get a unique identifier for lock-only transactions
   */
  const TxnIdReplicaIdPair transaction_id_replica_id() const;
  static const TxnIdReplicaIdPair transaction_id_replica_id(const Transaction*);

private:
  ReusableRequest txn_request_;
  std::optional<uint32_t> worker_;
  std::vector<ReusableRequest> early_remote_reads_;
  std::vector<std::pair<Key, LockMode>> keys_in_partition_;
  std::unordered_set<uint32_t> involved_partitions_;
  std::unordered_set<uint32_t> active_partitions_;
  std::unordered_set<uint32_t> involved_replicas_;
};

} // namespace slog