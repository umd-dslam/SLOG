#pragma once

#include <unordered_set>
#include <unordered_map>

#include "common/mmessage.h"
#include "common/types.h"
#include "proto/internal.pb.h"

using std::pair;
using std::string;
using std::unordered_map;
using std::unordered_set;

namespace slog {

inline internal::MachineId MakeMachineIdProto(uint32_t replica, uint32_t partition) {
  internal::MachineId machine_id;  
  machine_id.set_replica(replica);
  machine_id.set_partition(partition);
  return machine_id;
}

inline std::string MakeMachineId(uint32_t replica, uint32_t partition) {
  return std::to_string(replica) + ":" + std::to_string(partition);
}

/**
 * Creates a new transaction
 * @param read_set        Read set of the transaction
 * @param write_set       Write set of the transaction
 * @param code            Code of the transaction (not to be executed)
 * @param master_metadata Metadata regarding its mastership. This is used for
 *                        testing purpose.
 * @return                A new transaction having given properties
 */
Transaction MakeTransaction(
    const unordered_set<Key>& read_set,
    const unordered_map<Key, Value>& write_set,
    const string& code = "",
    const unordered_map<Key, pair<uint32_t, uint32_t>>& master_metadata = {});

/**
 * This function inspects the internal metadata of a transaction then
 * determines whether a transaction is SINGLE_HOME, MULTI_HOME, or UNKNOWN.
 * Pre-condition: all keys in master_metadata exist in either write set or 
 * read set of the transaction
 * 
 * @param txn The questioned transaction. Its `type` property will also be
 *            set to the result.
 * @return    The type of the given transaction. 
 */
TransactionType SetTransactionType(Transaction& txn);

}