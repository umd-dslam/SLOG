#pragma once

#include <vector>

#include "common/configuration.h"
#include "common/types.h"
#include "proto/transaction.pb.h"

namespace slog {

using KeyList = std::vector<std::pair<Key, LockMode>>;

class TransactionHolder {
public:
  TransactionHolder();
  TransactionHolder(ConfigurationPtr config, Transaction* txn);
  ~TransactionHolder();

  void SetTransaction(ConfigurationPtr config, Transaction* txn);
  Transaction* GetTransaction() const;
  Transaction* ReleaseTransaction();

  /**
   * Adds the subset of keys from a LOCK_ONLY transaction. Should only be called
   * on holders of MULTI_HOME transactions.
   * @param  lo_txn The LOCK_ONLY transaction to add
   * @return        An identifier for the LOCK_ONLY transaction if it has keys in
   *                this partition. Otherwise null_opt.
   */
  bool AddLockOnlyTransaction(ConfigurationPtr config, Transaction* lo_txn, TxnReplicaId& txn_replica_id);

  void SetWorker(const std::string& worker);
  const std::string& GetWorker() const;

  const KeyList& KeysInPartition() const;
  const unordered_map<ReplicaId, KeyList>& KeysPerReplica() const;

  std::vector<internal::Request>& EarlyRemoteReads();

private:
  Transaction* txn_;
  string worker_;
  std::vector<internal::Request> early_remote_reads_;
  KeyList keys_in_partition_;
  std::unordered_map<ReplicaId, KeyList> keys_per_replica_;
};

} // namespace slog