#pragma once

#include <vector>

#include "common/configuration.h"
#include "common/types.h"
#include "proto/transaction.pb.h"

namespace slog {

using TxnIdReplicaIdPair = std::pair<uint32_t, uint32_t>;

class TransactionHolder {
public:
  TransactionHolder();
  TransactionHolder(ConfigurationPtr config, Transaction* txn);
  ~TransactionHolder();

  void SetTransaction(ConfigurationPtr config, Transaction* txn);
  Transaction* GetTransaction() const;
  Transaction* ReleaseTransaction();

  void SetWorker(const std::string& worker);
  const std::string& GetWorker() const;

  const std::vector<std::pair<Key, LockMode>>& KeysInPartition() const;
  const std::unordered_set<uint32_t>& InvolvedPartitions() const;
  const std::unordered_set<uint32_t>& ActivePartitions() const;
  const std::unordered_set<uint32_t>& InvolvedReplicas() const;

  std::vector<internal::Request>& EarlyRemoteReads();

  /**
   * Get the id of the replica where this transaction was added to the local log.
   * Should only be used for single-home and lock-only transactions.
   */
  uint32_t GetReplicaId() const;
  static uint32_t GetReplicaId(Transaction* txn);

  /**
   * Get a unique identifier for lock-only transactions
   */
  const TxnIdReplicaIdPair GetTransactionIdReplicaIdPair() const;
  static const TxnIdReplicaIdPair GetTransactionIdReplicaIdPair(Transaction*);

private:
  Transaction* txn_;
  string worker_;
  std::vector<internal::Request> early_remote_reads_;
  std::vector<std::pair<Key, LockMode>> keys_in_partition_;
  std::unordered_set<uint32_t> involved_partitions_;
  std::unordered_set<uint32_t> active_partitions_;
  std::unordered_set<uint32_t> involved_replicas_;
};

} // namespace slog