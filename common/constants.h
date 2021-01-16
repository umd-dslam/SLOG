#pragma once

#include <string>

#include "common/types.h"

namespace slog {

const auto kModuleTimeout = 1000ms;

const Channel kBrokerChannel = 0;
const Channel kServerChannel = 1;
const Channel kForwarderChannel = 2;
const Channel kSequencerChannel = 3;
const Channel kMultiHomeOrdererChannel = 4;
const Channel kInterleaverChannel = 5;
const Channel kSchedulerChannel = 6;
const Channel kLocalPaxos = 7;
const Channel kGlobalPaxos = 8;
// Always set to the last channel + 1
const Channel kMaxChannel = 9;

const uint32_t kMaxNumMachines = 1000;

const uint32_t kPaxosDefaultLeaderPosition = 0;

const size_t kLockTableSizeLimit = 1000000;

/****************************
 *      Statistic Keys
 ****************************/

/* Server */
const char TXN_ID_COUNTER[] = "txn_id_counter";
const char NUM_PENDING_RESPONSES[] = "num_pending_responses";
const char NUM_PARTIALLY_COMPLETED_TXNS[] = "num_partially_completed_txns";
const char PENDING_RESPONSES[] = "pending_responses";
const char PARTIALLY_COMPLETED_TXNS[] = "partially_completed_txns";

/* Scheduler */
const char ALL_TXNS[] = "all_txns";
const char NUM_ALL_TXNS[] = "num_all_txns";
const char NUM_LOCKED_KEYS[] = "num_locked_keys";
const char NUM_TXNS_WAITING_FOR_LOCK[] = "num_txns_waiting_for_lock";
const char NUM_LOCKS_WAITED_PER_TXN[] = "num_locks_waited_per_txn";
const char LOCK_TABLE[] = "lock_table";
}  // namespace slog