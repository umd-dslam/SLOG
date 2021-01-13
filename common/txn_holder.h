#pragma once

#include <optional>
#include <vector>

#include "common/configuration.h"
#include "common/types.h"
#include "proto/transaction.pb.h"

namespace slog {

using TxnIdReplicaIdPair = std::pair<uint32_t, uint32_t>;
using EnvelopePtr = std::unique_ptr<internal::Envelope>;

class TxnHolder {
 public:
  TxnHolder(const ConfigurationPtr& config, Transaction* txn);

  Transaction* transaction() const { return txn_; }
  const std::vector<std::pair<Key, LockMode>>& keys_in_partition() const;
  const std::vector<uint32_t>& active_partitions() const;
  const std::vector<uint32_t>& involved_replicas() const;

  void SetWorker(uint32_t worker) { worker_ = worker; }
  std::optional<uint32_t> worker() const { return worker_; }

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
  Transaction* txn_;
  std::optional<uint32_t> worker_;
  std::vector<std::pair<Key, LockMode>> keys_in_partition_;
  std::vector<uint32_t> active_partitions_;
  std::vector<uint32_t> involved_replicas_;
};

}  // namespace slog