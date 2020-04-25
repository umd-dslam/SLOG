#pragma once

#include <unordered_set>
#include <unordered_map>

#include "common/configuration.h"
#include "common/mmessage.h"
#include "common/types.h"
#include "proto/internal.pb.h"

#define ENUM_NAME(enum, enum_type) enum_type##_descriptor()->FindValueByNumber(enum)->name()
#define CASE_NAME(case, type) type::descriptor()->FindFieldByNumber(case)->name()

using std::pair;
using std::string;
using std::unordered_map;
using std::unordered_set;

namespace slog {

internal::MachineId MakeMachineId(uint32_t replica, uint32_t partition);
internal::MachineId MakeMachineId(const string& machine_id_str);

string MakeMachineIdAsString(uint32_t replica, uint32_t partition);
string MakeMachineIdAsString(const internal::MachineId& machine_id);

/**
 * Creates a new transaction
 * @param read_set            Read set of the transaction
 * @param write_set           Write set of the transaction
 * @param code                Code of the transaction (not to be executed)
 * @param master_metadata     Metadata regarding its mastership. This is used for
 *                            testing purpose.
 * @param coordinating_server MachineId of the server in charge of responding the
 *                            transaction result to the client.
 * @return                    A new transaction having given properties
 */
Transaction* MakeTransaction(
    const unordered_set<Key>& read_set,
    const unordered_set<Key>& write_set,
    const string& code = "",
    const unordered_map<Key, pair<uint32_t, uint32_t>>& master_metadata = {},
    const internal::MachineId coordinating_server = MakeMachineId("0:0"),
    const uint32_t new_master = -1);

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

template<typename TxnOrBatch>
inline void RecordTxnEvent(ConfigurationPtr config, TxnOrBatch txn, TransactionEvent event) {
  txn->mutable_events()->Add(event);
  txn->mutable_event_times()->Add(
      duration_cast<microseconds>(
          Clock::now()
              .time_since_epoch()).count()
  );
  txn->mutable_event_machines()->Add(
      config->GetLocalMachineIdAsNumber());
}

} // namespace slog