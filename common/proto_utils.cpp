#include "proto_utils.h"

using std::to_string;

namespace slog {

SlogIdentifier MakeSlogId(uint32_t replica, uint32_t partition) {
  SlogIdentifier slog_id;
  slog_id.set_replica(replica);
  slog_id.set_partition(partition);
  return slog_id;
}

std::string SlogIdToString(const SlogIdentifier slog_id) {
  return to_string(slog_id.replica()) + ":" + to_string(slog_id.partition());
}

}