#pragma once

#include <unordered_set>
#include <unordered_map>

#include "common/mmessage.h"
#include "common/types.h"
#include "proto/internal.pb.h"

#define ENUM_NAME(enum, enum_type) enum_type##_descriptor()->FindValueByNumber(enum)->name()

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

inline internal::MachineId MakeMachineIdProto(const string& machine_id_str) {
  auto split = machine_id_str.find(':');
  if (split == string::npos) {
    throw std::invalid_argument("Invalid machine id: " + machine_id_str);
  }
  try {
    auto replica_str = machine_id_str.substr(0, split);
    auto partition_str = machine_id_str.substr(split + 1);

    internal::MachineId machine_id;
    machine_id.set_replica(std::stoul(replica_str));
    machine_id.set_partition(std::stoul(partition_str));
    return machine_id;
  } catch (...) {
    throw std::invalid_argument("Invalid machine id: " + machine_id_str);
  }
}

inline string MakeMachineId(uint32_t replica, uint32_t partition) {
  return std::to_string(replica) + ":" + std::to_string(partition);
}

inline string MakeMachineId(const internal::MachineId& machine_id) {
  return MakeMachineId(machine_id.replica(), machine_id.partition());
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
    const unordered_set<Key>& write_set,
    const string& code = "",
    const unordered_map<Key, pair<uint32_t, uint32_t>>& master_metadata = {},
    const internal::MachineId coordinating_server = MakeMachineIdProto("0:0"));

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

void MergeTransaction(Transaction& txn, const Transaction& other);

std::ostream& operator<<(std::ostream& os, const Transaction& txn);
bool operator==(const Transaction& txn1, const Transaction txn2);

} // namespace slog