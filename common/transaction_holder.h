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

  std::vector<internal::Request>& EarlyRemoteReads();

  const uint32_t GetReplicaId() const;
  const TxnIdReplicaIdPair GetTransactionIdReplicaIdPair() const;

  static const uint32_t GetReplicaId(Transaction* txn);
  static const TxnIdReplicaIdPair GetTransactionIdReplicaIdPair(Transaction*);

private:
  Transaction* txn_;
  string worker_;
  std::vector<internal::Request> early_remote_reads_;
  std::vector<std::pair<Key, LockMode>> keys_in_partition_;
};

} // namespace slog