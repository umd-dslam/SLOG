#pragma once

#include <optional>
#include <unordered_map>
#include <vector>

#include "common/configuration.h"
#include "common/sharder.h"
#include "common/types.h"

#define ENUM_NAME(enum, enum_type) enum_type##_descriptor()->FindValueByNumber(enum)->name()
#define CASE_NAME(case, type) type::descriptor()->FindFieldByNumber(case)->name()

using std::pair;
using std::string;
using std::unordered_map;

namespace slog {

struct KeyMetadata {
  KeyMetadata(const Key& key, KeyType type = KeyType::READ, std::optional<Metadata> metadata = {})
      : key(key), type(type), metadata(metadata) {}

  KeyMetadata(const Key& key, KeyType type, uint32_t master) : KeyMetadata(key, type, {{master}}) {}

  Key key;
  KeyType type;
  std::optional<Metadata> metadata;
};

/**
 * Creates a new transaction
 * @param key_metadatas       Keys and metadata
 * @param code                Code
 * @param remaster            If specify, `code` is ignored and the txn becomes a remaster txn
 * @param coordinating_server MachineId of the server in charge of responding the
 *                            transaction result to the client.
 * @return                    A new transaction having given properties
 */
Transaction* MakeTransaction(const std::vector<KeyMetadata>& key_metadatas,
                             const std::vector<std::vector<std::string>>& code = {{}},
                             std::optional<int> remaster = std::nullopt, MachineId coordinating_server = 0);

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
 * If in_place is set to true, the given txn is modified
 */
Transaction* GenerateLockOnlyTxn(Transaction* txn, uint32_t lo_master, bool in_place = false);

/**
 * Returns nullptr if the generated txn contains no relevant key
 */
Transaction* GeneratePartitionedTxn(const SharderPtr& sharder, Transaction* txn, uint32_t partition,
                                    bool in_place = false);

/**
 * Populate the involved_replicas field in the transaction
 */
void PopulateInvolvedReplicas(Transaction& txn);

/**
 * Populate the involved_partitions field in the transaction
 */
void PopulateInvolvedPartitions(const SharderPtr& sharder, Transaction& txn);

/**
 * Merges the results of two transactions
 *
 * @param txn   The transaction that will hold the final merged result
 * @param other The transaction to be merged with
 */
void MergeTransaction(Transaction& txn, const Transaction& other);

std::ostream& operator<<(std::ostream& os, const Transaction& txn);
std::ostream& operator<<(std::ostream& os, const MasterMetadata& metadata);

bool operator==(const MasterMetadata& metadata1, const MasterMetadata& metadata2);
bool operator==(const ValueEntry& val1, const ValueEntry& val2);
bool operator==(const KeyValueEntry& kv1, const KeyValueEntry& kv2);
bool operator==(const Transaction& txn1, const Transaction txn2);

/**
 * Extract txns from a batch
 */
std::vector<Transaction*> Unbatch(internal::Batch* batch);

}  // namespace slog