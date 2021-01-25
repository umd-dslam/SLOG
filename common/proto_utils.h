#pragma once

#include <unordered_map>
#include <variant>
#include <vector>

#include "common/configuration.h"
#include "common/types.h"
#include "proto/internal.pb.h"

#define ENUM_NAME(enum, enum_type) enum_type##_descriptor()->FindValueByNumber(enum)->name()
#define CASE_NAME(case, type) type::descriptor()->FindFieldByNumber(case)->name()

using std::pair;
using std::string;
using std::unordered_map;

namespace slog {

/**
 * Creates a new transaction
 * @param read_set            Read set of the transaction
 * @param write_set           Write set of the transaction
 * @param proc                Code or new master
 * @param master_metadata     Metadata regarding its mastership. This is used for
 *                            testing purpose.
 * @param coordinating_server MachineId of the server in charge of responding the
 *                            transaction result to the client.
 * @return                    A new transaction having given properties
 */
Transaction* MakeTransaction(const std::vector<Key>& read_set, const std::vector<Key>& write_set,
                             const std::variant<string, int>& proc = "",
                             const unordered_map<Key, pair<uint32_t, uint32_t>>& master_metadata = {},
                             MachineId coordinating_server = 0);

/**
 * If in_place is set to true, the given txn is modified
 */
Transaction* GenerateLockOnlyTxn(Transaction& txn, uint32_t lo_master, bool in_place = false);

/**
 * Populate the involved_replicas field in the transaction
 */
void PopulateInvolvedReplicas(Transaction* txn);

/**
 * Populate the involved_partitions field in the transaction
 */
void PopulateInvolvedPartitions(Transaction* txn, const ConfigurationPtr& config);

/**
 * Inspects the internal metadata of a transaction then determines whether
 * a transaction is SINGLE_HOME, MULTI_HOME, or UNKNOWN.
 * Pre-condition: all keys in master_metadata exist in either write set or
 * read set of the transaction
 *
 * @param txn The questioned transaction. Its `type` property will also be
 *            set to the result.
 * @return    The type of the given transaction.
 */
TransactionType SetTransactionType(Transaction& txn);

/**
 * Merges the results of two transactions
 *
 * @param txn   The transaction that will hold the final merged result
 * @param other The transaction to be merged with
 */
void MergeTransaction(Transaction& txn, const Transaction& other);

std::ostream& operator<<(std::ostream& os, const Transaction& txn);
bool operator==(const Transaction& txn1, const Transaction txn2);

std::ostream& operator<<(std::ostream& os, const MasterMetadata& metadata);

template <typename List>
void AddToPartitionedBatch(std::vector<std::unique_ptr<internal::Batch>>& partitioned_batch, const List& partitions,
                           Transaction* txn) {
  if (partitions.size() == 0) {
    return;
  }
  for (int i = 0; i < partitions.size() - 1; i++) {
    partitioned_batch[partitions[i]]->add_transactions()->CopyFrom(*txn);
  }
  partitioned_batch[partitions[partitions.size() - 1]]->mutable_transactions()->AddAllocated(txn);
}

/**
 * Extract txns from a batch
 */
vector<Transaction*> Unbatch(internal::Batch* batch);

}  // namespace slog